from celery import Celery
from src.config import REDIS_URL

celery_app = Celery(
    "telemetry_tasks",
    broker=REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Retry / reliability
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    task_time_limit=30,
    task_soft_time_limit=25,
)

celery_app.autodiscover_tasks(["src"])
