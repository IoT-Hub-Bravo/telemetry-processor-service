from typing import Any, List
import logging

from src.producers.producers_manager import TelemetryProducers
from src.serializers.telemetry_serializer import TelemetryBatchSerializer
from src.services.telemetry_services import telemetry_validate
from src.config import MAX_RETRIES

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

        retry_payloads = []

        for item in result.retry_data:
            payload = item["payload"]
            retry_count = payload.get("retry_count", 0)

            if retry_count >= MAX_RETRIES:
                self.producers.produce_dlq({
                    **payload,
                    "error": "max_retries_exceeded"
                })
                continue

            retry_payloads.append({
                **payload,
                "retry_count": retry_count + 1,
            })

        if retry_payloads:
            self.producers.produce_retry(retry_payloads)

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