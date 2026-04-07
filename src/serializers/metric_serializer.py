from iot_hub_shared.serializer_kit.json_serializer import JSONSerializer
from iot_hub_shared.utils_kit.normalization import normalize_str

class MetricSerializer(JSONSerializer):
    REQUIRED_FIELDS = {
        "name": str,
    }

    OPTIONAL_FIELDS = {
        "value": (int, float, bool, str),
        "unit": str,
        "type": str,
    }

    def _validate_fields(self, data: dict) -> dict:
        name = normalize_str(data.get("name"))
        value = data.get("value")
        unit = data.get("unit")
        metric_type = data.get("type")

        if not name:
            self._errors["name"] = "Metric name cannot be empty."

        if value is None and metric_type is None:
            self._errors["non_field_errors"] = "Either 'value' or 'type' must be provided."

        if value is not None and metric_type is not None:
            self._errors["non_field_errors"] = "Provide either 'value' or 'type', not both."

        if isinstance(value, str):
            value = normalize_str(value, allow_blank=False)
            if value is None:
                self._errors["value"] = "Value cannot be blank."

        return {
            "name": name,
            "value": value,
            "unit": unit,
            "type": metric_type,
        }