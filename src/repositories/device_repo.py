from sqlalchemy.orm import Session
from src.models.device import Device


class DeviceRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_serial(self, serial_number: str) -> Device | None:
        return (
            self.db.query(Device).filter(Device.serial_number == serial_number).first()
        )

    def create(self, serial_number: str, created_at=None) -> Device:
        device = Device(serial_number=serial_number, created_at=created_at)
        self.db.add(device)
        self.db.flush()
        return device

    def deactivate(self, device: Device):
        device.is_active = False
