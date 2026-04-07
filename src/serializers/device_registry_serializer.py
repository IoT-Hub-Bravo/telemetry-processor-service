
from typing import Any

from iot_hub_shared.serializer_kit.json_serializer import JSONSerializer
from iot_hub_shared.serializer_kit.base_serializer import BaseSerializer

from iot_hub_shared.utils_kit.normalization import normalize_str, parse_iso8601_utc
from src.serializers.metric_serializer import MetricSerializer


class DeviceRegistryItemSerializer(JSONSerializer):
    REQUIRED_FIELDS = {
        "device_serial_id": str,
        "metrics": list,
    }

    OPTIONAL_FIELDS = {
        "created_at": (str, type(None)),
        "is_active": (bool, type(None)),
    }

    def _validate_fields(self, data: dict) -> dict:
        serial = normalize_str(data.get("device_serial_id"))
        metrics = data.get("metrics") or []
        created_raw = data.get("created_at")
        is_active_raw = data.get("is_active")

        if not serial:
            self._errors["device_serial_id"] = "Device serial cannot be empty."

        created_at = None
        if created_raw:
            created_at = parse_iso8601_utc(created_raw)
            if created_at is None:
                self._errors["created_at"] = "Invalid ISO8601 timestamp."

        if is_active_raw is None:
            is_active = True
        elif isinstance(is_active_raw, bool):
            is_active = is_active_raw
        else:
            self._errors["is_active"] = "Must be a boolean."
            is_active = None

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

            if name in seen:
                metric_errors.append({idx: "Duplicate metric name."})
                continue

            seen.add(name)
            validated_metrics.append(metric_data)

        if metric_errors:
            self._errors["metrics"] = metric_errors

        return {
            "device_serial_id": serial,
            "created_at": created_at,
            "is_active": is_active,
            "metrics": validated_metrics,
        }    

class DeviceRegistryBatchSerializer(BaseSerializer):
    def _validate(self, data: Any):
        if not isinstance(data, list):
            self._errors["non_field_errors"] = "Payload must be a list."
            return None

        valid_items = []
        invalid_items = []

        for idx, item in enumerate(data):
            serializer = DeviceRegistryItemSerializer(item)

            if not serializer.is_valid():
                invalid_items.append({
                    "index": idx,
                    "errors": serializer.errors,
                    "payload": item,
                })
                continue

            valid_items.append(serializer.validated_data)

        return {
            "valid": valid_items,
            "invalid": invalid_items,
        }