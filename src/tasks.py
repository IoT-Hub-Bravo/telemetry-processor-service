# src/tasks/retry_tasks.py

from datetime import datetime

from src.celery_app import celery_app

from src.producers.producers import get_telemetry_retry_producer


from copy import deepcopy


@celery_app.task(bind=True, max_retries=5)
def retry_telemetry(self, payload: dict):
    try:
        record = deepcopy(payload)

        if "ts" in record and isinstance(record["ts"], datetime):
            record["ts"] = record["ts"].isoformat()

        producer = get_telemetry_retry_producer()
        producer.produce([record], key="retry_count")
        producer.flush(timeout=5)

    except Exception as exc:
        raise self.retry(
            exc=exc, countdown=2**self.request.retries  # exponential backoff
        )
