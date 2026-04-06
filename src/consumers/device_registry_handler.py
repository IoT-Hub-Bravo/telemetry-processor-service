from src.database import SessionLocal
from src.services.device_registry import update_database


class DeviceRegistryHandler:
    def handle(self, message: dict):
        db = SessionLocal()

        try:
            update_database(db, message)

        except Exception as e:
            db.rollback()
            raise e

        finally:
            db.close()