from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from src.database import Base

class DeviceMetric(Base):
    __tablename__ = "device_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    device_id = Column(UUID(as_uuid=True), ForeignKey("devices.id"), nullable=False)
    metric_id = Column(Integer, ForeignKey("metrics.id"), nullable=False)

    device = relationship("Device", backref="device_metrics")
    metric = relationship("Metric", backref="device_metrics")