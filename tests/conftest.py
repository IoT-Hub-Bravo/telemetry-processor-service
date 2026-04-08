# tests/conftest.py
import pytest
import uuid
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database import Base
from src.models.device import Device
from src.models.metric import Metric
from src.models.device_metric import DeviceMetric

# --- Engine & session fixtures ---
@pytest.fixture(scope="session")
def engine():
    engine = create_engine("sqlite:///:memory:", echo=False, future=True)
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture
def session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()

# --- Fake Redis checker fixture ---
class FakeChecker:
    def __init__(self):
        self.seen = set()
    def process(self, key: str) -> bool:
        if key in self.seen:
            return False
        self.seen.add(key)
        return True

@pytest.fixture
def mock_checker(monkeypatch):
    checker = FakeChecker()
    from src.utils.checker.redis_checker import build_redis_checker
    monkeypatch.setattr("src.utils.checker.redis_checker.build_redis_checker", lambda: checker)
    return checker

# --- Factory functions ---
def create_device(session, serial=None):
    serial = serial or f"dev-{uuid.uuid4()}"
    device = Device(id=uuid.uuid4(), serial_number=serial)
    session.add(device)
    session.commit()
    return device

def create_metric(session, name=None, data_type="numeric", unit=None):
    name = name or f"metric-{uuid.uuid4()}"
    metric = Metric(name=name, data_type=data_type, unit=unit)
    session.add(metric)
    session.commit()
    return metric

def create_device_metric(session, device: Device, metric: Metric):
    dm = DeviceMetric(device_id=device.id, metric_id=metric.id)
    session.add(dm)
    session.commit()
    return dm