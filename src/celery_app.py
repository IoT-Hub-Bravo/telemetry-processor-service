from celery import Celery
from src.config import REDIS_CELERY_URL, REDIS_URL

app = Celery(
    "telemetry_service",
    broker=REDIS_URL,
    backend=REDIS_CELERY_URL,
)

app.autodiscover_tasks(["src"])
app.conf.worker_prefetch_multiplier = 1
app.conf.task_acks_late = True
app.conf.result_backend = None