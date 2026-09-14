from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue


class SyntheticFleetStreamer:
    """Deterministic CDC stream for unit tests (8 valid events, 2 DLQ-bound)."""

    def __init__(self, kafka: InProcessKafkaQueue, *, topic: str) -> None:
        self.kafka = kafka
        self.topic = topic
        self.source_path = "synthetic://fleet-rental-cdc"
        self.current_offset = 0
        self._events = self._build_events()
        self._prefetched = False

    def _build_events(self) -> list[dict[str, Any]]:
        base = datetime(2026, 9, 1, 8, 0, 0, tzinfo=UTC)

        def ev(
            *,
            offset_sec: int,
            op: str,
            event_type: str,
            rental_id: str,
            vehicle_id: str,
            customer_id: str,
            location_id: str = "LOC-SFO",
            daily_rate: float = 79.99,
            vehicle_class: str = "compact",
            status: str,
        ) -> dict[str, Any]:
            ts = base + timedelta(seconds=offset_sec)
            return {
                "op": op,
                "event_time": ts.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event_type": event_type,
                "rental_id": rental_id,
                "vehicle_id": vehicle_id,
                "customer_id": customer_id,
                "location_id": location_id,
                "daily_rate": daily_rate,
                "vehicle_class": vehicle_class,
                "status": status,
            }

        return [
            ev(
                offset_sec=0,
                op="c",
                event_type="reservation_started",
                rental_id="RNT-SYN-001",
                vehicle_id="VEH-SYN-001",
                customer_id="CUS-SYN-001",
                status="reserved",
            ),
            ev(
                offset_sec=60,
                op="u",
                event_type="checkout_complete",
                rental_id="RNT-SYN-001",
                vehicle_id="VEH-SYN-001",
                customer_id="CUS-SYN-001",
                status="rented",
            ),
            ev(
                offset_sec=120,
                op="u",
                event_type="vehicle_returned",
                rental_id="RNT-SYN-001",
                vehicle_id="VEH-SYN-001",
                customer_id="CUS-SYN-001",
                status="available",
            ),
            ev(
                offset_sec=180,
                op="c",
                event_type="reservation_started",
                rental_id="RNT-SYN-002",
                vehicle_id="VEH-SYN-002",
                customer_id="CUS-SYN-002",
                status="reserved",
            ),
            ev(
                offset_sec=240,
                op="c",
                event_type="reservation_started",
                rental_id="RNT-SYN-003",
                vehicle_id="VEH-SYN-003",
                customer_id="CUS-SYN-003",
                status="reserved",
            ),
            ev(
                offset_sec=300,
                op="u",
                event_type="checkout_complete",
                rental_id="RNT-SYN-003",
                vehicle_id="VEH-SYN-003",
                customer_id="CUS-SYN-003",
                status="rented",
            ),
            ev(
                offset_sec=360,
                op="u",
                event_type="reservation_cancelled",
                rental_id="RNT-SYN-004",
                vehicle_id="VEH-SYN-004",
                customer_id="CUS-SYN-004",
                status="available",
            ),
            ev(
                offset_sec=420,
                op="u",
                event_type="vehicle_available",
                rental_id="RNT-SYN-005",
                vehicle_id="VEH-SYN-005",
                customer_id="CUS-SYN-005",
                status="available",
            ),
            {
                "op": "c",
                "event_time": (base + timedelta(seconds=480)).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event_type": "reservation_started",
                "rental_id": "RNT-SYN-BAD",
                "vehicle_id": "VEH-SYN-BAD",
                "customer_id": "CUS-SYN-BAD",
                "location_id": "LOC-SFO",
                "daily_rate": -10.0,
                "vehicle_class": "compact",
                "status": "reserved",
            },
            {
                "op": "x",
                "event_time": (base + timedelta(seconds=500)).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event_type": "reservation_started",
                "rental_id": "RNT-SYN-BAD2",
                "vehicle_id": "VEH-SYN-BAD2",
                "customer_id": "CUS-SYN-BAD2",
                "location_id": "LOC-SFO",
                "daily_rate": 59.99,
                "vehicle_class": "compact",
                "status": "reserved",
            },
        ]

    def _prefetch_to_kafka(self) -> None:
        if self._prefetched:
            return
        for event in self._events:
            self.kafka.publish(self.topic, event)
            self.current_offset += 1
        self._prefetched = True

    def get_next_event(self) -> dict[str, Any] | None:
        if not self._prefetched:
            self._prefetch_to_kafka()
        return self.kafka.consume(self.topic)
