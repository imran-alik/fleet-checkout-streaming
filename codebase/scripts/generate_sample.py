#!/usr/bin/env python
"""Generate committed fleet_rental_cdc.jsonl sample (200 valid CDC events)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data" / "source" / "samples" / "fleet_rental_cdc.jsonl"

LOCATIONS = ["LOC-SFO", "LOC-LAX", "LOC-SEA", "LOC-DEN"]
CLASSES = ["economy", "compact", "suv", "luxury"]
BASE_RATES = {"economy": 59.99, "compact": 79.99, "suv": 109.99, "luxury": 149.99}


def _event(
    *,
    ts: datetime,
    op: str,
    event_type: str,
    rental_id: str,
    vehicle_id: str,
    customer_id: str,
    location_id: str,
    daily_rate: float,
    vehicle_class: str,
    status: str,
) -> dict:
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


def build_events(target_count: int = 200) -> list[dict]:
    base = datetime(2026, 9, 1, 8, 0, 0, tzinfo=UTC)
    events: list[dict] = []
    sec = 0

    def append(ev: dict) -> None:
        nonlocal sec
        if len(events) >= target_count:
            return
        events.append(ev)
        sec += 2

    # 25 full rental cycles (3 events each = 75)
    for i in range(25):
        if len(events) >= target_count:
            break
        vclass = CLASSES[i % len(CLASSES)]
        rate = BASE_RATES[vclass]
        rid = f"RNT-{1000 + i:04d}"
        vid = f"VEH-{2000 + i:04d}"
        cid = f"CUS-{3000 + i:04d}"
        loc = LOCATIONS[i % len(LOCATIONS)]
        t0 = base + timedelta(seconds=sec)
        append(_event(ts=t0, op="c", event_type="reservation_started", rental_id=rid, vehicle_id=vid,
                      customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass, status="reserved"))
        append(_event(ts=t0 + timedelta(seconds=60), op="u", event_type="checkout_complete", rental_id=rid,
                      vehicle_id=vid, customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass,
                      status="rented"))
        append(_event(ts=t0 + timedelta(seconds=3600), op="u", event_type="vehicle_returned", rental_id=rid,
                      vehicle_id=vid, customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass,
                      status="available"))

    # 30 abandoned reservations (1 event each = 30)
    for i in range(30):
        if len(events) >= target_count:
            break
        vclass = CLASSES[(i + 1) % len(CLASSES)]
        rate = BASE_RATES[vclass]
        rid = f"RNT-A{i:03d}"
        vid = f"VEH-A{i:03d}"
        cid = f"CUS-A{i:03d}"
        loc = LOCATIONS[i % len(LOCATIONS)]
        t0 = base + timedelta(seconds=sec)
        append(_event(ts=t0, op="c", event_type="reservation_started", rental_id=rid, vehicle_id=vid,
                      customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass, status="reserved"))

    # 15 maintenance cycles (2 events = 30)
    for i in range(15):
        if len(events) >= target_count:
            break
        vclass = "suv"
        rate = BASE_RATES[vclass]
        rid = f"RNT-M{i:03d}"
        vid = f"VEH-M{i:03d}"
        cid = f"CUS-M{i:03d}"
        loc = LOCATIONS[i % len(LOCATIONS)]
        t0 = base + timedelta(seconds=sec)
        append(_event(ts=t0, op="c", event_type="vehicle_maintenance", rental_id=rid, vehicle_id=vid,
                      customer_id=cid, location_id=loc, daily_rate=0.0, vehicle_class=vclass, status="maintenance"))
        append(_event(ts=t0 + timedelta(seconds=7200), op="u", event_type="vehicle_available", rental_id=rid,
                      vehicle_id=vid, customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass,
                      status="available"))

    # 20 cancelled reservations (2 events = 40)
    for i in range(20):
        if len(events) >= target_count:
            break
        vclass = CLASSES[i % len(CLASSES)]
        rate = BASE_RATES[vclass]
        rid = f"RNT-C{i:03d}"
        vid = f"VEH-C{i:03d}"
        cid = f"CUS-C{i:03d}"
        loc = LOCATIONS[i % len(LOCATIONS)]
        t0 = base + timedelta(seconds=sec)
        append(_event(ts=t0, op="c", event_type="reservation_started", rental_id=rid, vehicle_id=vid,
                      customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass, status="reserved"))
        append(_event(ts=t0 + timedelta(seconds=30), op="u", event_type="reservation_cancelled", rental_id=rid,
                      vehicle_id=vid, customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass,
                      status="available"))

    # Pad with vehicle_available heartbeat events until 200
    pad_i = 0
    while len(events) < target_count:
        vclass = CLASSES[pad_i % len(CLASSES)]
        rate = BASE_RATES[vclass]
        rid = f"RNT-P{pad_i:03d}"
        vid = f"VEH-P{pad_i:03d}"
        cid = f"CUS-P{pad_i:03d}"
        loc = LOCATIONS[pad_i % len(LOCATIONS)]
        t0 = base + timedelta(seconds=sec)
        append(_event(ts=t0, op="c", event_type="vehicle_available", rental_id=rid, vehicle_id=vid,
                      customer_id=cid, location_id=loc, daily_rate=rate, vehicle_class=vclass, status="available"))
        pad_i += 1

    return events[:target_count]


def main() -> int:
    events = build_events(200)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    print(f"Wrote {len(events)} events to {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
