import logging
from src.database import SessionLocal
from src.services.device_registry import update_database
from src.serializers.device_registry_serializer import DeviceRegistryBatchSerializer

logger = logging.getLogger(__name__)

class DeviceRegistryHandler:
    def handle(self, messages):
        if not isinstance(messages, list):
            messages = [messages]

        serializer = DeviceRegistryBatchSerializer(messages)

        if not serializer.is_valid():
            logger.error(f"Batch validation failed: {serializer.errors}")
            return

        result = serializer.validated_data

        valid_items = result["valid"]
        invalid_items = result["invalid"]

        if invalid_items:
            for item in invalid_items:
                logger.error(
                    f"Invalid message at index {item['index']}: "
                    f"errors={item['errors']} payload={item['payload']}"
                )

        if not valid_items:
            return

        db = SessionLocal()

        try:
            for item in valid_items:
                update_database(db, item)

            db.commit()

        except Exception:
            db.rollback()
            raise

        finally:
            db.close()
