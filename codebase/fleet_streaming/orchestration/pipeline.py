from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any, Protocol

from pydantic import ValidationError

from fleet_streaming.config.settings import PipelineConfig
from fleet_streaming.ingestion.checkpoint import IngestCheckpoint
from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue
from fleet_streaming.quality.dlq import DeadLetterQueue
from fleet_streaming.schemas.events import RentalCdcEvent
from fleet_streaming.state.redis_store import LocalRedisStore
from fleet_streaming.warehouse.duckdb_loader import DuckDBWarehouse

log = logging.getLogger(__name__)


class EventStreamer(Protocol):
    source_path: str
    current_offset: int

    def get_next_event(self) -> dict[str, Any] | None: ...


class FleetStreamingPipeline:
    def __init__(
        self,
        config: PipelineConfig,
        streamer: EventStreamer,
        *,
        kafka: InProcessKafkaQueue | None = None,
    ) -> None:
        config.ensure_dirs()
        self.config = config
        self.streamer = streamer
        self.kafka = kafka or InProcessKafkaQueue()
        self.redis = LocalRedisStore(default_ttl_seconds=config.reservation_hold_ttl_seconds)
        self.warehouse = DuckDBWarehouse(
            config.db_path,
            config.bronze_table,
            config.silver_table,
            config.gold_table,
            batch_size=config.duckdb_batch_size,
        )
        self.dlq = DeadLetterQueue(config.dlq_dir)
        self.checkpoint = IngestCheckpoint.load(config.checkpoint_path, source_path=streamer.source_path)
        self.triggers_fired: list[dict[str, Any]] = []
        self.events_processed = 0

    def _save_checkpoint(self) -> None:
        self.checkpoint.source_path = self.streamer.source_path
        self.checkpoint.offset = self.streamer.current_offset
        self.checkpoint.save(self.config.checkpoint_path)

    def validate_event(self, raw_event_dict: dict[str, Any]) -> RentalCdcEvent:
        if raw_event_dict.get("daily_rate") is not None and raw_event_dict.get("daily_rate") < 0:
            raise ValueError("Data Quality Breach: daily_rate must be non-negative.")
        return RentalCdcEvent(**raw_event_dict)

    def _update_inventory(self, validated: RentalCdcEvent) -> None:
        key = f"inventory:{validated.vehicle_id}"
        payload = {
            "vehicle_id": validated.vehicle_id,
            "rental_id": validated.rental_id,
            "status": validated.status,
            "location_id": validated.location_id,
            "vehicle_class": validated.vehicle_class,
            "daily_rate": validated.daily_rate,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        event_type = validated.event_type

        if event_type == "reservation_started":
            self.redis.set_with_ttl(
                key,
                payload,
                ttl_seconds=self.config.reservation_hold_ttl_seconds,
            )
        elif event_type in {"checkout_complete", "vehicle_returned", "vehicle_available"}:
            self.redis.set_persistent(key, payload)
            if event_type == "checkout_complete":
                self.redis.delete(f"hold:{validated.rental_id}")
        elif event_type == "vehicle_maintenance":
            self.redis.set_persistent(key, {**payload, "status": "maintenance"})
        elif event_type == "reservation_cancelled":
            self.redis.set_persistent(key, {**payload, "status": "available"})

    def process_incoming_event(self, raw_event_dict: dict[str, Any]) -> None:
        try:
            validated = self.validate_event(raw_event_dict)
        except (ValidationError, ValueError) as exc:
            self.dlq.route(raw_event_dict, str(exc))
            return

        self._update_inventory(validated)
        self.warehouse.enqueue(validated)
        self.events_processed += 1
        self._save_checkpoint()

    def _fire_trigger(self, campaign_type: str, data: dict[str, Any]) -> None:
        payload = {"campaign_type": campaign_type, "data": data}
        self.triggers_fired.append(payload)
        log.info("[TRIGGER] %s -> %s", campaign_type, data)

    def run(self, *, max_events: int | None = None, ttl_wait_seconds: float | None = None) -> dict[str, Any]:
        wait_seconds = (
            ttl_wait_seconds
            if ttl_wait_seconds is not None
            else self.config.reservation_hold_ttl_seconds + 1
        )
        processed = 0
        while True:
            raw_event = self.streamer.get_next_event()
            if raw_event is None:
                break
            self.process_incoming_event(raw_event)
            processed += 1
            if max_events is not None and processed >= max_events:
                break
            if self.config.stream_delay_seconds > 0:
                time.sleep(self.config.stream_delay_seconds)

        if wait_seconds > 0:
            time.sleep(wait_seconds)

        for key, data in self.redis.pop_expired():
            if not key.startswith("inventory:"):
                continue
            if data.get("status") != "reserved":
                continue
            trigger_key = f"trigger:hold_expired:{data['rental_id']}"
            if self.redis.mark_triggered(trigger_key):
                self._fire_trigger(
                    "Reservation Hold Expired",
                    {
                        "rental_id": data["rental_id"],
                        "vehicle_id": data["vehicle_id"],
                        "vehicle_class": data.get("vehicle_class"),
                        "daily_rate": data.get("daily_rate"),
                    },
                )

        self.warehouse.flush()
        silver_rows = self.warehouse.build_silver()
        gold_kpis = self.warehouse.build_gold()
        self._save_checkpoint()
        summary = self.summary()
        summary["silver_rows"] = silver_rows
        summary["gold_kpis"] = gold_kpis
        summary["inventory_snapshot"] = self.redis.inventory_snapshot()
        return summary

    def summary(self) -> dict[str, Any]:
        return {
            "events_processed": self.events_processed,
            "warehouse_rows": self.warehouse.count(),
            "dlq_files": self.dlq.count(),
            "triggers_fired": len(self.triggers_fired),
            "triggers": list(self.triggers_fired),
            "checkpoint_offset": self.streamer.current_offset,
            "source_path": self.streamer.source_path,
        }

    def close(self) -> None:
        self.warehouse.close()
