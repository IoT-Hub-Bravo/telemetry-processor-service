from typing import Any

from iot_hub_shared.serializer_kit.base_serializer import BaseSerializer
from iot_hub_shared.serializer_kit.json_serializer import JSONSerializer
from iot_hub_shared.utils_kit.normalization import normalize_str
from iot_hub_shared.utils_kit.normalization import parse_iso8601_utc
from src.serializers.metric_serializer import MetricSerializer


class TelemetryItemSerializer(JSONSerializer):
    REQUIRED_FIELDS = {
        "device_serial_id": str,
        "metrics": list,
    }

    OPTIONAL_FIELDS = {"ts": (str, type(None)), "retry_count": int}

    def _validate_fields(self, data: dict) -> dict:
        serial = normalize_str(data.get("device_serial_id"))
        metrics = data.get("metrics") or []
        ts_raw = data.get("ts")

        # --- serial ---
        if not serial:
            self._errors["device_serial_id"] = "Device serial cannot be empty."

        # --- ts ---
        ts = None
        if ts_raw:
            ts = parse_iso8601_utc(ts_raw)
            if ts is None:
                self._errors["ts"] = "Invalid ISO8601 timestamp."

        retry_count = data.get("retry_count", 0)

        if retry_count is not None and not isinstance(retry_count, int):
            self._errors["retry_count"] = "Must be integer"

        # --- metrics ---
        if not isinstance(metrics, list):
            self._errors["metrics"] = "Metrics must be a list."
            return {}

        validated_metrics = []
        metric_errors = []

        seen = set()

        for idx, metric in enumerate(metrics):
            serializer = MetricSerializer(metric)

            if not serializer.is_valid():
                metric_errors.append({idx: serializer.errors})
                continue

            metric_data = serializer.validated_data
            name = metric_data["name"]

            # duplicate check
            if name in seen:
                metric_errors.append({idx: "Duplicate metric name."})
                continue

            seen.add(name)
            validated_metrics.append(metric_data)

        if metric_errors:
            self._errors["metrics"] = metric_errors

        if retry_count is not None:
            return {
                "device_serial_id": serial,
                "ts": ts,
                "metrics": validated_metrics,
                "retry_count": retry_count,
            }

        return {
            "device_serial_id": serial,
            "ts": ts,
            "metrics": validated_metrics,
        }


class TelemetryBatchSerializer(BaseSerializer):
    def _validate(self, data: Any):
        if not isinstance(data, list):
            self._errors["non_field_errors"] = "Payload must be a list."
            return None

        validated_items = []
        errors = []

        for idx, item in enumerate(data):
            serializer = TelemetryItemSerializer(item)

            if not serializer.is_valid():
                errors.append({idx: serializer.errors})
                continue

            validated_items.append(serializer.validated_data)

        if errors:
            self._errors["items"] = errors
            return None

        return validated_items
