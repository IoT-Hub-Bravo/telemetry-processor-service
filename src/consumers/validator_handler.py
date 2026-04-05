from typing import Any, List
from src.producers.producers_manager import TelemetryProducers
from src.serializers.telemetry_serializer import TelemetryBatchSerializer
from src.services.telemetry_services import telemetry_validate

class ValidatorPayloadHandler:
    def __init__(self, producers: TelemetryProducers):
        self.producers = producers

    def normalize_payload(self, payload: dict | list, source: str = "unknown") -> list | None:
        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            return payload
        logger.error("payload must be dict or list, got %s", type(payload).__name__)
        return None

    def handle(self, payloads: List[Any]) -> None:
        normalized = []
        for payload in payloads:
            p = self.normalize_payload(payload)
            if p:
                normalized.extend(p)

        if not normalized:
            return

        serializer = TelemetryBatchSerializer(normalized)
        serializer.is_valid()

        validation_result = telemetry_validate(payload=serializer.validated_data)

        if validation_result.validated_rows:
            self.producers.produce_clean(validation_result.validated_rows)
        if validation_result.errors:
            self.producers.produce_dlq(validation_result.errors)
        if validation_result.expired_rows:
            self.producers.produce_expired(validation_result.expired_rows)