from __future__ import annotations

import hashlib


def deterministic_event_id(
    *,
    rental_id: str,
    event_time: str,
    event_type: str,
    vehicle_id: str,
    op: str,
) -> str:
    """Stable idempotency key for at-least-once CDC transports."""
    canonical = "|".join([rental_id, event_time, event_type, vehicle_id, op])
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"cdc_{digest}"
