# tests/test_validator_extended.py
import pytest
from datetime import datetime, timedelta, timezone
from src.services.telemetry_validator import TelemetryBatchValidator
from tests.conftest import create_device_metric, create_device, create_metric

@pytest.mark.parametrize("value", [10, 0, -5, 3.14])
def test_valid_numeric_payload(session, mock_checker, value):
    device = create_device(session)
    metric = create_metric(session, data_type="numeric")
    create_device_metric(session, device, metric)

    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": metric.name, "value": value, "unit": None}]}]

    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data == []
    assert len(validator.validated_data) == 1

def test_valid_bool_payload(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session, data_type="bool")
    create_device_metric(session, device, metric)

    for val in [True, False]:
        payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                    "metrics": [{"name": metric.name, "value": val, "unit": None}]}]
        validator = TelemetryBatchValidator(payload)
        validator.validate(session.get_bind())
        assert validator.invalid_data == []
        assert len(validator.validated_data) == 1

def test_valid_str_payload(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session, data_type="str")
    create_device_metric(session, device, metric)

    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": metric.name, "value": "hello", "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data == []
    assert len(validator.validated_data) == 1

def test_missing_device(session, mock_checker):
    payload = [{"device_serial_id": "unknown", "ts": datetime.now(timezone.utc), "metrics": [{"name": "m1", "value": 1, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.retry_data[0]["error"] == "device_not_found"

def test_metric_not_configured(session, mock_checker):
    device = create_device(session)
    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc), "metrics": [{"name": "m1", "value": 1, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.retry_data[0]["error"] == "metric_not_configured"

def test_unit_mismatch(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session, unit="C")
    create_device_metric(session, device, metric)
    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": metric.name, "value": 25, "unit": "F"}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data[0]["error"] == "unit_mismatch"

def test_type_mismatch(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session, data_type="numeric")
    create_device_metric(session, device, metric)
    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": metric.name, "value": "wrong", "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data[0]["error"] == "type_mismatch"

def test_duplicate_across_payload(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session)
    create_device_metric(session, device, metric)

    ts = datetime.now(timezone.utc)
    payload = [
        {"device_serial_id": device.serial_number, "ts": ts, "metrics": [{"name": metric.name, "value": 1, "unit": None}]},
        {"device_serial_id": device.serial_number, "ts": ts, "metrics": [{"name": metric.name, "value": 1, "unit": None}]}
    ]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    errors = [e for e in validator.invalid_data if e["error"] == "duplicate"]
    assert len(errors) == 1

def test_expired_timestamp(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session)
    create_device_metric(session, device, metric)
    ts_old = datetime.now(timezone.utc) - timedelta(seconds=3600)
    payload = [{"device_serial_id": device.serial_number, "ts": ts_old,
                "metrics": [{"name": metric.name, "value": 5, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert len(validator.expired_data) == 1

def test_optional_ts_none(session, mock_checker):
    device = create_device(session)
    metric = create_metric(session)
    create_device_metric(session, device, metric)
    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": metric.name, "value": 1, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data == []

def test_empty_metrics_list(session, mock_checker):
    device = create_device(session)
    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc), "metrics": []}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    # Should be skipped, not fail
    assert validator.validated_data == []

def test_multiple_metrics_some_invalid(session, mock_checker):
    device = create_device(session)
    m1 = create_metric(session)
    m2 = create_metric(session)
    create_device_metric(session, device, m1)
    # m2 not linked, should fail
    payload = [{"device_serial_id": device.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": m1.name, "value": 1, "unit": None},
                            {"name": m2.name, "value": 2, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert any(e["error"] == "metric_not_configured" for e in validator.retry_data)
    assert len(validator.validated_data) == 1

def test_multiple_devices(session, mock_checker):
    d1 = create_device(session)
    d2 = create_device(session)
    m1 = create_metric(session)
    m2 = create_metric(session)
    create_device_metric(session, d1, m1)
    create_device_metric(session, d2, m2)
    payload = [
        {"device_serial_id": d1.serial_number, "ts": datetime.now(timezone.utc), "metrics": [{"name": m1.name, "value": 1, "unit": None}]},
        {"device_serial_id": d2.serial_number, "ts": datetime.now(timezone.utc), "metrics": [{"name": m2.name, "value": 2, "unit": None}]}
    ]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert len(validator.validated_data) == 2

def test_metric_name_empty(session, mock_checker):
    d = create_device(session)
    m = create_metric(session)
    create_device_metric(session, d, m)
    payload = [{"device_serial_id": d.serial_number, "ts": datetime.now(timezone.utc), "metrics": [{"name": "", "value": 1, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.retry_data[0]["error"] == "metric_not_configured"

def test_value_blank_string_for_str_type(session, mock_checker):
    d = create_device(session)
    m = create_metric(session, data_type="str")
    create_device_metric(session, d, m)
    payload = [{"device_serial_id": d.serial_number, "ts": datetime.now(timezone.utc), "metrics": [{"name": m.name, "value": "  ", "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data[0]["error"] == "type_mismatch"

def test_value_bool_for_numeric(session, mock_checker):
    d = create_device(session)
    m = create_metric(session, data_type="numeric")
    create_device_metric(session, d, m)
    payload = [{"device_serial_id": d.serial_number, "ts": datetime.now(timezone.utc), "metrics": [{"name": m.name, "value": True, "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    assert validator.invalid_data[0]["error"] == "type_mismatch"

def test_multiple_invalid_entries(session, mock_checker):
    d = create_device(session)
    m1 = create_metric(session, data_type="numeric", unit="C")
    m2 = create_metric(session, data_type="bool")
    create_device_metric(session, d, m1)
    create_device_metric(session, d, m2)
    payload = [{"device_serial_id": d.serial_number, "ts": datetime.now(timezone.utc),
                "metrics": [{"name": m1.name, "value": "wrong", "unit": "F"},
                            {"name": m2.name, "value": "notbool", "unit": None}]}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    errors = [e["error"] for e in validator.invalid_data]
    assert "type_mismatch" in errors and "unit_mismatch" in errors

def test_payload_with_missing_metrics_key(session, mock_checker):
    d = create_device(session)
    payload = [{"device_serial_id": d.serial_number, "ts": datetime.now(timezone.utc)}]
    validator = TelemetryBatchValidator(payload)
    validator.validate(session.get_bind())
    # metrics is None => skipped
    assert validator.validated_data == []