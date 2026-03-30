from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Union
from datetime import datetime

app = FastAPI()


class MetricInput(BaseModel):
    name: str
    value: Union[int, float, str, bool]
    unit: str


class TelemetryInput(BaseModel):
    device_serial: str
    ts: datetime
    metrics: List[MetricInput]


class TelemetryOutput(BaseModel):
    device_serial_id: str
    device_metric_id: int
    ts: datetime
    type: str
    value: Union[int, float, str, bool]


FAKE_DEVICES = {"SN-0001"}

FAKE_DEVICE_METRICS = {
    "SN-0001": {
        "temperature": {"id": 1, "data_type": "numeric", "unit": "celsius"},
        "humidity": {"id": 2, "data_type": "numeric", "unit": "percent"},
    }
}


def validate(payload: List[TelemetryInput]):
    valid = []
    invalid = []

    for item in payload:
        serial = item.device_serial

        if serial not in FAKE_DEVICES:
            invalid.append({"device": serial, "error": "device_not_found"})
            continue

        for metric in item.metrics:
            dm = FAKE_DEVICE_METRICS.get(serial, {}).get(metric.name)

            if not dm:
                invalid.append({"metric": metric.name, "error": "metric_not_configured"})
                continue

            valid.append({
                "device_serial_id": serial,
                "device_metric_id": dm["id"],
                "ts": item.ts,
                "type": dm["data_type"],
                "value": metric.value,
            })

    return valid, invalid


@app.post("/validate", response_model=dict)
def validate_telemetry(payload: List[TelemetryInput]):
    valid, invalid = validate(payload)

    return {
        "valid": valid,
        "invalid": invalid,
    }


@app.get("/health")
def health():
    return {"status": "ok"}