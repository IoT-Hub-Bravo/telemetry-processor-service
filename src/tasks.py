import logging
from typing import Any
import time

from celery import shared_task, group
from iot_hub_shared.kafka_kit.producer import ProduceResult

from src.serializers.telemetry_serializer import TelemetryBatchSerializer
from src.services.telemetry_services import telemetry_validate
from src.producers import (
    get_telemetry_clean_producer,
    get_telemetry_dlq_producer,
    get_telemetry_expired_producer,
    get_telemetry_retry_producer
)

# from iot_hub_shared.observability_kit.metrics import (
#     ingestion_messages_total,
#     ingestion_latency_seconds,
#     ingestion_errors_total,
# )

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    # autoretry_for=(OperationalError, InterfaceError),
    retry_backoff=True,
    retry_backoff_max=10,
    retry_jitter=True,
    retry_kwargs={"max_retries": 10},
)
def validate_telemetry_payload(self, payload: dict | list) -> dict:
    start_time = time.perf_counter()
    source = 'kafka'

    payload = normalize_payload(payload)

    if payload is None:
        ingestion_errors_total.labels(source=source, error_type='invalid_payload').inc()
        ingestion_messages_total.labels(source=source, status='error').inc()
        return {"valid": 0, "errors": 1, "expired": 0}

    serializer = TelemetryBatchSerializer(payload)
    serializer.is_valid()

    if not serializer.valid_items:
        error_count = len(serializer.item_errors) if serializer.item_errors else len(payload)
        # ingestion_errors_total.labels(source=source, error_type='validation_error').inc(
        #     error_count
        # )
        # ingestion_messages_total.labels(source=source, status='error').inc(error_count)
        # logger.warning("Telemetry validation rejected: no valid items.")
        # latency = time.perf_counter() - start_time
        # ingestion_latency_seconds.labels(source=source).observe(latency)
        return serializer.item_errors

    validation_result = telemetry_validate(payload=serializer.valid_items)

    valid_count = len(validation_result.validated_rows)
    error_count = len(validation_result.errors)

    # if valid_count > 0:
    #     ingestion_messages_total.labels(source=source, status='success').inc(valid_count)
    # if error_count > 0:
    #     ingestion_errors_total.labels(source=source, error_type='validation_error').inc(
    #         error_count
    #     )
    #     ingestion_messages_total.labels(source=source, status='error').inc(error_count)

    # latency = time.perf_counter() - start_time
    # ingestion_latency_seconds.labels(source=source).observe(latency)

    logger.info(
        "Validation completed: received=%d, valid=%d, invalid=%d",
        len(payload),
        valid_count,
        error_count,
    )
    produce_validation_results(validation_result)
    return {
        "valid": valid_count,
        "errors": error_count,
        "expired": len(validation_result.expired_rows),
    }

def normalize_payload(payload: dict | list, source: str = 'unknown') -> list | None:
    """
    Normalize payload to list.
    Returns None if invalid type.
    """
    if isinstance(payload, dict):
        return [payload]

    if isinstance(payload, list):
        return payload

    ingestion_errors_total.labels(source=source, error_type='invalid_payload').inc()
    logger.error(f"payload must be of type dict or list, got {type(payload).__name__}")
    return

def produce_validation_results(validation_result) -> None:
    """
    Produce validation results into corresponding Kafka topics.
    """
    group(
        produce_data.s("clean", validation_result.validated_rows),
        produce_data.s("dlq", validation_result.errors),
        produce_data.s("expired", validation_result.expired_rows),
    ).delay()

@shared_task
def produce_data(producer_type: str, data: list[dict[str, Any]]) -> None:
    """
    Produce batch of records to Kafka.
    """
    if producer_type == "clean":
        producer = get_telemetry_clean_producer()
    elif producer_type == "dlq":
        producer = get_telemetry_dlq_producer()
    elif producer_type == "expired":
        producer = get_telemetry_expired_producer()

    accepted = 0
    errors = {}
    for index, record in enumerate(data):
        payload = {**record, "ts": record["ts"].isoformat()}
        result = producer.produce(
            payload=payload,
            key=record.get("device_serial_id"),
        )
        if result == ProduceResult.ENQUEUED:
            accepted += 1
        else:
            errors[index] = result.value
    logger.info(
        "Produced %d/%d messages to topic %s",
        accepted,
        len(data),
        producer.topic,
    )
    if errors:
        logger.warning("Producer errors: %s", errors)

    producer.flush()

