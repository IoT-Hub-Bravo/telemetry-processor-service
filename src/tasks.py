# src/tasks/retry_tasks.py

from src.celery_app import celery_app
from src.producers.producers_manager import TelemetryProducers

producers = TelemetryProducers()


@celery_app.task(bind=True, max_retries=5)
def retry_telemetry(self, payload: dict):
    try:
        producers.produce_raw(payload)

    except Exception as exc:
        raise self.retry(
            exc=exc,
            countdown=2 ** self.request.retries  # exponential backoff
        )