from dataclasses import dataclass, field
from src.services.telemetry_validator import TelemetryBatchValidator

import logging

logger = logging.getLogger(__name__)

@dataclass(slots=True)
class TelemetryValidationResult:
    validated_rows: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)
    expired_rows: list[dict] = field(default_factory=list)


def telemetry_validate(payload: dict | list[dict]) -> TelemetryValidationResult:
    if isinstance(payload, dict):
        payload_list = [payload]
    else:
        payload_list = payload

    validator = TelemetryBatchValidator(payload=payload_list)
    validator.validate()

    if validator.errors:
        logger.warning(
            "Telemetry validation completed with errors for %d items. Errors: %s",
            len(payload),
            validator.errors,
        )

    logger.info(
        "Telemetry validation completed. %d valid rows ready for creation.",
        len(validator.validated_rows),
    )

    return TelemetryValidationResult(
        validated_rows=validator.validated_rows,
        errors=validator.invalid_rows,
        expired_rows=validator.expired_rows,
    )
