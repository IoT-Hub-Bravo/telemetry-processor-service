import logging
from dataclasses import dataclass, field
from src.services.telemetry_validator import TelemetryBatchValidator
from src.database import db_engine

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TelemetryValidationResult:
    validated_data: list[dict] = field(default_factory=list)
    invalid_data: list[dict] = field(default_factory=list)
    retry_data: list[dict] = field(default_factory=list)
    expired_data: list[dict] = field(default_factory=list)


def telemetry_validate(payload: dict | list[dict]) -> TelemetryValidationResult:
    payload_list = payload if isinstance(payload, list) else [payload]

    validator = TelemetryBatchValidator(payload=payload_list)
    validator.validate(engine=db_engine)

    if validator.invalid_data:
        logger.warning(
            "Telemetry invalid items: %d",
            len(validator.invalid_data),
        )

    if validator.retry_data:
        logger.warning(
            "Telemetry retry items: %d",
            len(validator.retry_data),
        )

    logger.info(
        "Telemetry validation completed. valid=%d retry=%d invalid=%d expired=%d",
        len(validator.validated_data),
        len(validator.retry_data),
        len(validator.invalid_data),
        len(validator.expired_data),
    )

    return TelemetryValidationResult(
        validated_data=validator.validated_data,
        invalid_data=validator.invalid_data,
        retry_data=validator.retry_data,
        expired_data=validator.expired_data,
    )
