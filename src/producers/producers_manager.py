from typing import Any, List
import logging

from src.serializers.telemetry_serializer import TelemetryBatchSerializer
from src.services.telemetry_services import telemetry_validate
from src.producers.producers import (
    get_telemetry_clean_producer,
    get_telemetry_dlq_producer,
    get_telemetry_expired_producer,
    get_telemetry_retry_producer,
)
from iot_hub_shared.kafka_kit.producer import KafkaProducer

logger = logging.getLogger(__name__)


class TelemetryProducers:
    def __init__(self):
        self.clean: KafkaProducer = get_telemetry_clean_producer()
        self.dlq: KafkaProducer = get_telemetry_dlq_producer()
        self.expired: KafkaProducer = get_telemetry_expired_producer()
        self.retry: KafkaProducer = get_telemetry_retry_producer()

    def _send(self, producer: KafkaProducer, data: List[dict]):
        accepted = 0
        errors = {}
        for index, record in enumerate(data):
            payload = {**record, "ts": record["ts"].isoformat()}
            result = producer.produce(payload=payload, key=record.get("device_serial_id"))
            if result == producer.ENQUEUED:
                accepted += 1
            else:
                errors[index] = getattr(result, "value", str(result))
        if errors:
            logger.warning("Producer errors: %s", errors)
        logger.info("Produced %d/%d messages to topic %s", accepted, len(data), producer.topic)

    def produce_clean(self, data: List[dict]):
        self._send(self.clean, data)

    def produce_dlq(self, data: List[dict]):
        self._send(self.dlq, data)

    def produce_expired(self, data: List[dict]):
        self._send(self.expired, data)

    def produce_retry(self, data: List[dict]):
        self._send(self.retry, data)

    def flush_all(self):
        for producer in [self.clean, self.dlq, self.expired, self.retry]:
            producer.flush()
