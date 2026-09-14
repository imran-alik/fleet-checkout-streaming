from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, field_validator

from fleet_streaming.idempotency import deterministic_event_id

VALID_EVENT_TYPES = frozenset(
    {
        "reservation_started",
        "reservation_cancelled",
        "checkout_complete",
        "vehicle_returned",
        "vehicle_maintenance",
        "vehicle_available",
    }
)


class RentalCdcEvent(BaseModel):
    """Debezium-style rental fleet CDC contract."""

    cdc_id: str | None = None
    op: str
    event_time: str
    event_type: str
    rental_id: str
    vehicle_id: str
    customer_id: str
    location_id: str
    daily_rate: float
    vehicle_class: str
    status: str

    @field_validator("op")
    @classmethod
    def validate_op(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"c", "r", "u", "d"}:
            raise ValueError(f"Invalid CDC op: {value}")
        return normalized

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in VALID_EVENT_TYPES:
            raise ValueError(f"Unknown event_type: {value}")
        return normalized

    def model_post_init(self, __context: object) -> None:
        if not self.cdc_id:
            self.cdc_id = deterministic_event_id(
                rental_id=self.rental_id,
                event_time=self.event_time,
                event_type=self.event_type,
                vehicle_id=self.vehicle_id,
                op=self.op,
            )

    @property
    def daily_rate_decimal(self) -> Decimal:
        return Decimal(str(self.daily_rate))
