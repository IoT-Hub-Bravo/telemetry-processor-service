from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from src.models.device_metric import DeviceMetric


class DeviceMetricRepository:
    def __init__(self, db: Session):
        self.db = db

    def link(self, device_id, metric_id):
        stmt = (
            insert(DeviceMetric)
            .values(device_id=device_id, metric_id=metric_id)
            .on_conflict_do_nothing()
        )

        self.db.execute(stmt)

    def exists(self, device_id, metric_id) -> bool:
        return (
            self.db.query(DeviceMetric)
            .filter(
                DeviceMetric.device_id == device_id, DeviceMetric.metric_id == metric_id
            )
            .first()
            is not None
        )
