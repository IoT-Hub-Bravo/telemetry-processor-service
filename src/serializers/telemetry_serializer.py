from typing import Any

from iot_hub_shared.serializer_kit.base_serializer import BaseSerializer
from iot_hub_shared.serializer_kit.json_serializer import JSONSerializer
from iot_hub_shared.utils_kit.normalization import normalize_str
from iot_hub_shared.utils_kit.normalization import parse_iso8601_utc

class MetricSerializer(JSONSerializer):
    REQUIRED_FIELDS = {
        "name": str,
        "value": (int, float, bool, str),
    }

    OPTIONAL_FIELDS = {
        "unit": str,
    }

    def _validate_fields(self, data: dict) -> dict:
        name = normalize_str(data.get("name"))
        value = data.get("value")
        unit = data.get("unit")

        if not name:
            self._errors["name"] = "Metric name cannot be empty."

        if isinstance(value, str):
            value = normalize_str(value, allow_blank=False)
            if value is None:
                self._errors["value"] = "Value cannot be blank."

        return {
            "name": name,
            "value": value,
            "unit": unit,
        }

class TelemetryItemSerializer(JSONSerializer):
    REQUIRED_FIELDS = {
        "device_serial_id": str,
        "metrics": list,
    }

    OPTIONAL_FIELDS = {
        "ts": (str, type(None)),
    }

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