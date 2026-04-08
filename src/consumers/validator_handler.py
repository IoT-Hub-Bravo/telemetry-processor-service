from typing import Any, List
import logging
import random

from src.producers.producers_manager import TelemetryProducers
from src.serializers.telemetry_serializer import TelemetryBatchSerializer
from src.services.telemetry_services import telemetry_validate
from src.config import MAX_RETRIES

from src.tasks import retry_telemetry

logger = logging.getLogger(__name__)


class ValidatorPayloadHandler:
    def __init__(self, producers: TelemetryProducers):
        self.producers = producers

    def normalize_payload(self, payload: dict | list) -> list | None:
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            return payload

        logger.error("payload must be dict or list, got %s", type(payload).__name__)
        return None

    def handle(self, payloads: List[Any]) -> None:

        normalized = [
            item
            for payload in payloads
            for item in (self.normalize_payload(payload) or [])
        ]

        if not normalized:
            return

        serializer = TelemetryBatchSerializer(normalized)

        if not serializer.is_valid():
            logger.error("Serializer validation failed: %s", serializer.errors)
            return

        result = telemetry_validate(payload=serializer.validated_data)

        if result.validated_data:
            self.producers.produce_clean(result.validated_data)

        for item in result.retry_data:
            payload = item["payload"]
            error = item.get("error")
            retry_count = payload.get("retry_count", 0)

            if retry_count >= MAX_RETRIES:
                self.producers.produce_dlq({
                    **payload,
                    "error": "max_retries_exceeded",
                    "last_error": error,
                })
                continue

            next_retry_count = retry_count + 1

            # exponential backoff + jitter
            delay = (2 ** retry_count) + random.randint(0, 3)

            retry_payload = {
                **payload,
                "retry_count": next_retry_count,
            }

            retry_telemetry.apply_async(
                args=[retry_payload],
                countdown=delay
            )

            logger.info(
                "scheduled_retry",
                extra={
                    "retry_count": next_retry_count,
                    "delay": delay,
                    "error": error,
                },
            )

        if result.invalid_data:
            self.producers.produce_dlq(result.invalid_data)

        if result.expired_data:
            self.producers.produce_expired(result.expired_data)

        logger.info(
            "telemetry_handler_completed",
            extra={
                "valid": len(result.validated_data),
                "retry": len(result.retry_data),
                "invalid": len(result.invalid_data),
                "expired": len(result.expired_data),
            },
        )