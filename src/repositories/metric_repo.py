from sqlalchemy.orm import Session
from src.models.metric import Metric


class MetricRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_name(self, name: str) -> Metric | None:
        return self.db.query(Metric).filter(Metric.name == name).first()

    def create(self, name: str, data_type: str, unit: str | None):
        metric = Metric(name=name, data_type=data_type, unit=unit)
        self.db.add(metric)
        self.db.flush()
        return metric
