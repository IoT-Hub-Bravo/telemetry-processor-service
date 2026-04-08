from src.utils.checker.checker_config import RedisConfig
from src.utils.checker.idempotency_store import RedisIdempotencyStore
from src.utils.checker.duplicate_checker import DuplicateChecker
from src.config import REDIS_HOST, REDIS_PORT

redis_host = REDIS_HOST
redis_port = REDIS_PORT


def build_redis_checker() -> DuplicateChecker:
    redis_config = RedisConfig(host=redis_host, port=redis_port)
    redis_client = redis_config.create_client()

    store = RedisIdempotencyStore(redis_client=redis_client)
    return DuplicateChecker(store=store)
