from collections import defaultdict
from typing import Any

import BaseValidator
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models.device import Device
from src.models.device_metric import DeviceMetric
from src.models.metric import Metric
from src.config import TELEMETRY_MAX_AGE_SECONDS
from src.utils.unit_aliases import REVERSE_UNIT_ALIASES
from src.utils.checker.redis_checker import build_redis_checker


class TelemetryBatchValidator(BaseValidator):
    def __init__(self, payload: list[dict]):
        self._payload = payload
        self._retry_data = []
        self._invalid_data = []
        self._validated_data = []
        self._expired_data = []

    @property
    def invalid_data(self):
        return self._invalid_data
    
    @property
    def validated_data(self):
        return self._validated_data
    
    @property
    def expired_data(self):
        return self._expired_data
    
    def validate(self):
        self._validated_rows.clear()
        self._invalid_rows.clear()

        self._load_devices_and_metrics()

        self._validate_payload()

        self._split_expired()

        self._validate_duplicates()

    def _load_devices(self, session: Session):
        device_serials = {
            item.get("device_serial")
            for item in self._payload
            if item.get("device_serial")
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
            select(Device.serial_number, Metric.name)
            .join(DeviceMetric, Device.id == DeviceMetric.device_id)
            .join(Metric, Metric.id == DeviceMetric.metric_id)
            .where(Device.serial_number.in_(self._existing_devices))
        )

        device_metric_map = defaultdict(set)

        for serial, metric_name in session.execute(stmt):
            device_metric_map[serial].add(metric_name)

        self._device_metric_map = dict(device_metric_map)

    def _load_devices_and_metrics(self):
        self._load_devices()
        self._load_device_metrics()

    def _validate_payload(self) -> None:
        for index, item in enumerate(self._initial_data):
            serial = item.get("device_serial_id")
            metrics = item.get("metrics") or []
            ts = item.get("ts")

            # device check
            if serial not in self._validated_devices:
                self._add_invalid_record(
                    index=index,
                    serial=serial,
                    ts=ts,
                    metric=None,
                    value=None,
                    unit=None,
                    error="device_not_found",
                )
                continue

            device_metrics_map = self._initial_device_metrics.get(serial, {})

            for metric in metrics:
                metric_name = metric.get("name")
                value = metric.get("value")
                unit = metric.get("unit")

                # basic metric structure validation
                if not metric_name:
                    self._add_invalid_record(
                        index=index,
                        serial=serial,
                        ts=ts,
                        metric=None,
                        value=value,
                        unit=unit,
                        error="missing_metric_name",
                    )
                    continue

                device_metric_data = device_metrics_map.get(metric_name)

                if not device_metric_data:
                    self._add_invalid_record(
                        index=index,
                        serial=serial,
                        ts=ts,
                        metric=metric_name,
                        value=value,
                        unit=unit,
                        error="metric_not_configured",
                    )
                    continue

                normalized_payload_unit = self._normalize_unit(unit)
                normalized_db_unit = self._normalize_unit(device_metric_data["unit"])

                if normalized_payload_unit != normalized_db_unit:
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
        """
        Check for duplicate telemetry entries using Redis-based DuplicateChecker.
        Moves duplicates to _invalid_rows and keeps only unique validated rows.
        """
        checker = build_redis_checker()
        unique_valid_items = []

        for index, item in enumerate(self._validated_rows):
            dm_id = item.get("device_metric_id")
            ts = item.get("ts")
            serial = item.get("device_serial_id")
            value = item.get("value_jsonb", {}).get("v")

            result = checker.process(f"{dm_id},{ts}")
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

        self._validated_rows = unique_valid_items

    def _value_matches_data_type(self, value: Any, data_type: str) -> bool:
        type_checkers = {
            "numeric": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
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
        now = timezone.now()
        threshold = now - timedelta(seconds=window_seconds)

        fresh = []
        expired = []

        for item in self._validated_data:
            ts = item.get("ts")

            if ts is None:
                fresh.append(item)
                continue

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


