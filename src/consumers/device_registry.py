import os
import signal

import django
from decouple import config

from iot_hub_shared.kafka_kit.kafka_consumer import KafkaConsumer
from iot_hub_shared.kafka_kit.config import ConsumerConfig
from iot_hub_shared.kafka_kit.message_handlers import CeleryPayloadHandler


from src.tasks import update_database  # noqa

TOPIC = config('KAFKA_TOPIC_DEVICE_REGISTRY', default='device.registry')
CONSUME_TIMEOUT = config('KAFKA_CONSUMER_CONSUME_TIMEOUT', default=1.0, cast=float)
DECODE_JSON = config('KAFKA_CONSUMER_DECODE_JSON', default=True, cast=bool)
CONSUME_BATCH = config('KAFKA_CONSUMER_CONSUME_BATCH', default=True, cast=bool)
BATCH_MAX_SIZE = config('KAFKA_CONSUMER_BATCH_MAX_SIZE', default=100, cast=int)


def main():
    consumer = KafkaConsumer(
        config=ConsumerConfig(),
        topics=[TOPIC],
        handler=CeleryPayloadHandler(update_database),
        consume_timeout=CONSUME_TIMEOUT,
        decode_json=DECODE_JSON,
        consume_batch=CONSUME_BATCH,
        batch_max_size=BATCH_MAX_SIZE,
    )

    signal.signal(signal.SIGTERM, consumer.stop)
    signal.signal(signal.SIGINT, consumer.stop)

    consumer.start()


if __name__ == '__main__':
    main()
