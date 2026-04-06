from iot_hub_shared.serializer_kit.json_serializer import JSONSerializer
from iot_hub_shared.utils_kit.normalization import normalize_str

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