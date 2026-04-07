from datetime import datetime
from sqlalchemy.orm import Session

from src.repositories.device_repo import DeviceRepository
from src.repositories.metric_repo import MetricRepository
from src.repositories.device_metric_repo import DeviceMetricRepository


def update_database(db: Session, payload: dict):
    device_repo = DeviceRepository(db)
    metric_repo = MetricRepository(db)
    device_metric_repo = DeviceMetricRepository(db)

    serial = payload["device_serial_id"]

    created_at = None
    if payload.get("created_at"):
        created_at = payload["created_at"]

    # --- Device ---
    device = device_repo.get_by_serial(serial)

    if not device:
        device = device_repo.create(
            serial_number=serial,
            created_at=created_at
        )

    # --- Metrics ---
    for metric_data in payload.get("metrics", []):
        metric = metric_repo.get_by_name(metric_data["name"])

        if not metric:
            metric = metric_repo.create(
                name=metric_data["name"],
                data_type=metric_data["type"],
                unit=metric_data.get("unit"),
            )

        if not device_metric_repo.exists(device.id, metric.id):
            device_metric_repo.link(device.id, metric.id)

    db.commit()