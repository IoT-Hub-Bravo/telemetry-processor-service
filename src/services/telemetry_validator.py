from collections import defaultdict
from typing import Any
from datetime import datetime, timedelta, timezone

from iot_hub_shared.serializer_kit.base_validator import BaseValidator
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.device import Device
from src.models.device_metric import DeviceMetric
from src.models.metric import Metric
from src.config import TELEMETRY_MAX_AGE_SECONDS
from src.utils.unit_aliases import REVERSE_UNIT_ALIASES
from src.utils.checker.redis_checker import build_redis_checker


class TelemetryBatchValidator(BaseValidator):
    RETRYABLE_ERRORS = {"device_not_found", "metric_not_configured"}

    def __init__(self, payload: list[dict]):
        self._payload = payload
        self._errors = []
        self._checker = build_redis_checker()

        self._retry_data: list[dict] = []
        self._invalid_data: list[dict] = []
        self._validated_data: list[dict] = []
        self._expired_data: list[dict] = []

        self._existing_devices: set[str] = set()
        self._device_metric_map: dict[str, dict] = {}

    @property
    def invalid_data(self):
        return self._invalid_data

    @property
    def retry_data(self):
        return self._retry_data

    @property
    def validated_data(self):
        return self._validated_data

    @property
    def expired_data(self):
        return self._expired_data

    def validate(self, engine):
        self._validated_data.clear()
        self._invalid_data.clear()
        self._retry_data.clear()
        self._expired_data.clear()

        with Session(engine) as session:
            self._load_devices(session)
            self._load_device_metrics(session)

        self._validate_payload()
        self._split_expired()
        self._validate_duplicates()

    def _load_devices(self, session: Session):
        device_serials = {
            item.get("device_serial_id")
            for item in self._payload
            if item.get("device_serial_id")
        }

        if not device_serials:
            return

        stmt = select(Device.serial_number).where(
            Device.serial_number.in_(device_serials)
        )
        self._existing_devices = set(session.scalars(stmt).all())

    def _load_device_metrics(self, session: Session):
        if not self._existing_devices:
            return

        stmt = (
            select(
                Device.serial_number,
                Metric.name,
                Metric.unit,
                Metric.data_type,
                DeviceMetric.id,
            )
            .join(DeviceMetric, Device.id == DeviceMetric.device_id)
            .join(Metric, Metric.id == DeviceMetric.metric_id)
            .where(Device.serial_number.in_(self._existing_devices))
        )

        device_metric_map = defaultdict(dict)
        for serial, name, unit, data_type, dm_id in session.execute(stmt):
            device_metric_map[serial][name] = {
                "unit": self._normalize_unit(unit),
                "data_type": data_type,
                "device_metric_id": dm_id,
            }

        self._device_metric_map = dict(device_metric_map)

    def _validate_payload(self) -> None:
        for index, item in enumerate(self._payload):
            serial = item.get("device_serial_id")
            metrics = item.get("metrics") or []
            ts = item.get("ts")

            # --- device exists? ---
            if serial not in self._existing_devices:
                self._add_retry_item(index, item, "device_not_found")
                continue

            device_metrics_map = self._device_metric_map.get(serial, {})

            for metric in metrics:
                metric_name = metric.get("name")
                value = metric.get("value")
                unit = metric.get("unit")

                device_metric_data = device_metrics_map.get(metric_name)

                if not device_metric_data:
                    self._add_retry_item(index, item, "metric_not_configured")
                    continue

                normalized_payload_unit = self._normalize_unit(unit)

                if normalized_payload_unit != device_metric_data["unit"]:
                    self._add_invalid_record(
                        index=index,
                        serial=serial,
                        ts=ts,
                        metric=metric_name,
                        value=value,
                        unit=unit,
                        error="unit_mismatch",
                    )
                    continue

                if not self._value_matches_data_type(
                    value, device_metric_data["data_type"]
                ):
                    self._add_invalid_record(
                        index=index,
                        serial=serial,
                        ts=ts,
                        metric=metric_name,
                        value=value,
                        unit=unit,
                        error="type_mismatch",
                    )
                    continue

                self._validated_data.append(
                    {
                        "device_serial_id": serial,
                        "device_metric_id": device_metric_data["device_metric_id"],
                        "ts": ts,
                        "type": device_metric_data["data_type"],
                        "value": value,
                    }
                )

    def _validate_duplicates(self) -> None:
        """Check duplicates via Redis DuplicateChecker"""
        checker = self._checker
        unique_valid_items = []

        for index, item in enumerate(self._validated_data):
            dm_id = item.get("device_metric_id")
            ts = item.get("ts")
            serial = item.get("device_serial_id")
            value = item.get("value")

            ts_key = ts.isoformat() if ts else "null"
            key = f"{dm_id},{ts_key}" if ts_key != "null" else f"{dm_id},no_ts"
            result = checker.process(f"{dm_id},{key}")
            if not result:
                self._add_invalid_record(
                    index=index,
                    serial=serial,
                    ts=ts,
                    metric=None,
                    value=value,
                    unit=None,
                    error="duplicate",
                )
                continue

            unique_valid_items.append(item)

        self._validated_data = unique_valid_items

    def _value_matches_data_type(self, value: Any, data_type: str) -> bool:
        type_checkers = {
            "numeric": lambda v: isinstance(v, (int, float))
            and not isinstance(v, bool),
            "bool": lambda v: isinstance(v, bool),
            "str": lambda v: isinstance(v, str) and bool(v.strip()),
        }
        return type_checkers.get(data_type, lambda v: False)(value)

    def _normalize_unit(self, unit: str | None) -> str | None:
        if not unit:
            return None
        return REVERSE_UNIT_ALIASES.get(
            unit.strip().lower().replace("°", ""), unit.strip().lower()
        )

    def _split_expired(self) -> None:
        window_seconds = TELEMETRY_MAX_AGE_SECONDS
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(seconds=window_seconds)

        fresh = []
        expired = []

        for item in self._validated_data:
            ts = item.get("ts")
            if ts is None:
                fresh.append(item)
                continue

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
            item["ts"] = ts

            if ts < threshold:
                expired.append(item)
            else:
                fresh.append(item)

        self._validated_data = fresh
        self._expired_data = expired

    def _add_invalid_record(
        self,
        *,
        index: int | None,
        serial: str | None,
        ts: Any,
        metric: str | None,
        value: Any,
        unit: Any,
        error: str,
    ):
        self._invalid_data.append(
            {
                "index": index,
                "device_serial_id": serial,
                "ts": ts,
                "metric": metric,
                "value": value,
                "unit": unit,
                "error": error,
            }
        )

    def _add_retry_item(self, index: int, payload: dict, error: str):
        self._retry_data.append(
            {
                "index": index,
                "payload": payload,
                "error": error,
            }
        )
