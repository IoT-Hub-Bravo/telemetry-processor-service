import pytest
import uuid
from datetime import datetime, timedelta

from tests.conftest import create_device, create_metric
from src.repositories.device_repo import DeviceRepository
from src.repositories.metric_repo import MetricRepository
from src.repositories.device_metric_repo import DeviceMetricRepository
from src.models.device import Device
from src.models.metric import Metric
from src.models.device_metric import DeviceMetric
from sqlalchemy.exc import IntegrityError


def test_device_repository_create_and_get(session):
    repo = DeviceRepository(session)
    serial = f"dev-{uuid.uuid4()}"
    device = repo.create(serial)
    session.commit()
    fetched = repo.get_by_serial(serial)
    assert fetched is not None
    assert fetched.serial_number == serial


def test_device_created_at_custom(session):
    repo = DeviceRepository(session)
    ts = datetime.now() - timedelta(days=1)
    serial = f"dev-{uuid.uuid4()}"
    device = repo.create(serial, created_at=ts)
    session.commit()
    fetched = repo.get_by_serial(serial)
    assert abs(fetched.created_at - ts) < timedelta(seconds=1)


def test_device_deactivate(session):
    repo = DeviceRepository(session)
    serial = f"dev-{uuid.uuid4()}"
    device = repo.create(serial)
    session.commit()
    repo.deactivate(device)
    session.commit()
    fetched = repo.get_by_serial(serial)
    assert fetched.is_active is False


def test_metric_repository_create_and_get(session):
    repo = MetricRepository(session)
    name = f"metric-{uuid.uuid4()}"
    metric = repo.create(name, data_type="numeric", unit="C")
    session.commit()
    fetched = repo.get_by_name(name)
    assert fetched is not None
    assert fetched.name == name
    assert fetched.data_type == "numeric"
    assert fetched.unit == "C"


def test_metric_repository_various_types(session):
    repo = MetricRepository(session)
    types = ["numeric", "str", "bool"]
    for t in types:
        name = f"metric-{uuid.uuid4()}"
        m = repo.create(name, data_type=t, unit=None)
        session.commit()
        fetched = repo.get_by_name(name)
        assert fetched.data_type == t


def test_device_metric_link_and_exists(session):
    dev_repo = DeviceRepository(session)
    met_repo = MetricRepository(session)
    dm_repo = DeviceMetricRepository(session)

    device = dev_repo.create(f"dev-{uuid.uuid4()}")
    metric = met_repo.create(f"metric-{uuid.uuid4()}", data_type="numeric", unit="C")
    session.commit()

    dm_repo.link(device.id, metric.id)
    session.commit()
    assert dm_repo.exists(device.id, metric.id)


def test_device_metric_exists_false(session):
    dm_repo = DeviceMetricRepository(session)
    fake_id = uuid.uuid4()
    assert not dm_repo.exists(fake_id, fake_id)


def test_multiple_devices(session):
    repo = DeviceRepository(session)
    devices = [repo.create(f"dev-{uuid.uuid4()}") for _ in range(3)]
    session.commit()
    for d in devices:
        fetched = repo.get_by_serial(d.serial_number)
        assert fetched is not None


def test_multiple_metrics(session):
    repo = MetricRepository(session)
    metrics = [repo.create(f"metric-{uuid.uuid4()}", "numeric", None) for _ in range(3)]
    session.commit()
    for m in metrics:
        fetched = repo.get_by_name(m.name)
        assert fetched is not None


def test_device_metric_link_multiple(session):
    dev_repo = DeviceRepository(session)
    met_repo = MetricRepository(session)
    dm_repo = DeviceMetricRepository(session)

    device = dev_repo.create(f"dev-{uuid.uuid4()}")
    metrics = [met_repo.create(f"metric-{uuid.uuid4()}", "numeric", "C") for _ in range(3)]
    session.commit()

    for m in metrics:
        dm_repo.link(device.id, m.id)
    session.commit()

    for m in metrics:
        assert dm_repo.exists(device.id, m.id)


def test_create_device_duplicate_serial(session):
    repo = DeviceRepository(session)
    serial = f"dev-{uuid.uuid4()}"
    repo.create(serial)
    session.commit()
    with pytest.raises(IntegrityError):
        repo.create(serial)
        session.commit()
    session.rollback()


def test_metric_create_multiple_same_name(session):
    repo = MetricRepository(session)
    name = f"metric-{uuid.uuid4()}"
    repo.create(name, "numeric", None)
    session.commit()
    with pytest.raises(IntegrityError):
        repo.create(name, "str", "C")
        session.commit()
    session.rollback()


def test_link_nonexistent_device_or_metric(session):
    dm_repo = DeviceMetricRepository(session)

    device = create_device(session)
    metric = create_metric(session)

    session.delete(device)
    session.delete(metric)
    session.commit()

    dm_repo.link(device.id, metric.id)

    assert True


def test_exists_with_nonexistent_ids(session):
    dm_repo = DeviceMetricRepository(session)

    device = create_device(session)
    metric = create_metric(session)

    assert dm_repo.exists(device.id, metric.id) is False


def test_deactivate_multiple_devices(session):
    repo = DeviceRepository(session)
    devices = [repo.create(f"dev-{uuid.uuid4()}") for _ in range(3)]
    session.commit()
    for d in devices:
        repo.deactivate(d)
    session.commit()
    for d in devices:
        fetched = repo.get_by_serial(d.serial_number)
        assert not fetched.is_active


def test_metric_multiple_same_data_type(session):
    repo = MetricRepository(session)
    metrics = [repo.create(f"metric-{uuid.uuid4()}", "numeric", None) for _ in range(2)]
    session.commit()
    for m in metrics:
        fetched = repo.get_by_name(m.name)
        assert fetched.data_type == "numeric"


def test_metric_unit_variation(session):
    repo = MetricRepository(session)
    m1 = repo.create(f"metric-{uuid.uuid4()}", "numeric", "C")
    m2 = repo.create(f"metric-{uuid.uuid4()}", "numeric", "F")
    session.commit()
    assert repo.get_by_name(m1.name).unit == "C"
    assert repo.get_by_name(m2.name).unit == "F"


def test_device_serial_unique(session):
    repo = DeviceRepository(session)
    serial = f"dev-{uuid.uuid4()}"
    device1 = repo.create(serial)
    session.commit()
    with pytest.raises(IntegrityError):
        repo.create(serial)
        session.commit()
    session.rollback()


def test_metric_name_unique(session):
    repo = MetricRepository(session)
    name = f"metric-{uuid.uuid4()}"
    m1 = repo.create(name, "numeric", "C")
    session.commit()
    with pytest.raises(IntegrityError):
        repo.create(name, "str", "F")
        session.commit()
    session.rollback()


def test_device_metric_link_idempotent(session):
    dev_repo = DeviceRepository(session)
    met_repo = MetricRepository(session)
    dm_repo = DeviceMetricRepository(session)

    device = dev_repo.create(f"dev-{uuid.uuid4()}")
    metric = met_repo.create(f"metric-{uuid.uuid4()}", "numeric", "C")
    session.commit()

    dm_repo.link(device.id, metric.id)
    dm_repo.link(device.id, metric.id)
    session.commit()

    assert dm_repo.exists(device.id, metric.id)


def test_device_metric_multiple_links(session):
    dev_repo = DeviceRepository(session)
    met_repo = MetricRepository(session)
    dm_repo = DeviceMetricRepository(session)

    device1 = dev_repo.create(f"dev-{uuid.uuid4()}")
    device2 = dev_repo.create(f"dev-{uuid.uuid4()}")
    metric1 = met_repo.create(f"metric-{uuid.uuid4()}", "numeric", "C")
    metric2 = met_repo.create(f"metric-{uuid.uuid4()}", "numeric", "F")
    session.commit()

    dm_repo.link(device1.id, metric1.id)
    dm_repo.link(device1.id, metric2.id)
    dm_repo.link(device2.id, metric1.id)
    session.commit()

    assert dm_repo.exists(device1.id, metric1.id)
    assert dm_repo.exists(device1.id, metric2.id)
    assert dm_repo.exists(device2.id, metric1.id)
    assert not dm_repo.exists(device2.id, metric2.id)


def test_device_repository_fetch_nonexistent(session):
    repo = DeviceRepository(session)
    serial = f"nonexistent-{uuid.uuid4()}"
    assert repo.get_by_serial(serial) is None


def test_metric_repository_fetch_nonexistent(session):
    repo = MetricRepository(session)
    name = f"nonexistent-{uuid.uuid4()}"
    assert repo.get_by_name(name) is None


def test_device_created_default_active(session):
    repo = DeviceRepository(session)
    device = repo.create(f"dev-{uuid.uuid4()}")
    session.commit()
    assert device.is_active is True


def test_metric_repository_unit_none(session):
    repo = MetricRepository(session)
    metric = repo.create(f"metric-{uuid.uuid4()}", "numeric", None)
    session.commit()
    fetched = repo.get_by_name(metric.name)
    assert fetched.unit is None


def test_device_metric_exists_for_multiple_links(session):
    dev_repo = DeviceRepository(session)
    met_repo = MetricRepository(session)
    dm_repo = DeviceMetricRepository(session)

    device = dev_repo.create(f"dev-{uuid.uuid4()}")
    metrics = [met_repo.create(f"metric-{uuid.uuid4()}", "numeric", None) for _ in range(3)]
    session.commit()

    for m in metrics:
        dm_repo.link(device.id, m.id)
    session.commit()

    for m in metrics:
        assert dm_repo.exists(device.id, m.id)