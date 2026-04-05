import os
from decouple import config

TELEMETRY_MAX_AGE_SECONDS = config("TELEMETRY_MAX_AGE_SECONDS", default=300, cast=int)

DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "telemetry")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")

# async URL (FastAPI)
# TODO: Remove this
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# sync URL (Alembic)
ALEMBIC_DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

REDIS_HOST= os.getenv("REDIS_HOST", "redis")
REDIS_PORT= config("REDIS_PORT", default=6379, cast=int)
REDIS_DB = config("REDIS_DB", default=0, cast=int)

REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

TELEMETRY_CLEAN_TOPIC = config('KAFKA_TOPIC_TELEMETRY_CLEAN', default='telemetry.clean')
TELEMETRY_DLQ_TOPIC = config('KAFKA_TOPIC_TELEMETRY_DLQ', default='telemetry.dlq')
TELEMETRY_EXPIRED_TOPIC = config('KAFKA_TOPIC_TELEMETRY_EXPIRED', default='telemetry.expired')
TELEMETRY_RETRY_TOPIC = config('KAFKA_TOPIC_TELEMETRY_RETRY', default='telemetry.retry')