from __future__ import annotations

from pathlib import Path

import duckdb

from fleet_streaming.analytics.kpi import compute_gold_kpis
from fleet_streaming.config.settings import PipelineConfig
from fleet_streaming.idempotency import deterministic_event_id
from fleet_streaming.ingestion.cdc_jsonl import CdcJsonlReader, CdcJsonlStreamer
from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue
from fleet_streaming.ingestion.synthetic import SyntheticFleetStreamer
from fleet_streaming.orchestration.pipeline import FleetStreamingPipeline


def test_deterministic_event_id_is_stable() -> None:
    kwargs = {
        "rental_id": "RNT-001",
        "event_time": "2026-09-01 08:00:00 UTC",
        "event_type": "reservation_started",
        "vehicle_id": "VEH-1001",
        "op": "c",
    }
    assert deterministic_event_id(**kwargs) == deterministic_event_id(**kwargs)


def test_synthetic_pipeline_idempotent_on_replay(tmp_path: Path) -> None:
    config = PipelineConfig.for_tests(tmp_path)
    kafka = InProcessKafkaQueue()
    streamer = SyntheticFleetStreamer(kafka, topic=config.kafka_topic)

    pipeline = FleetStreamingPipeline(config, streamer, kafka=kafka)
    first = pipeline.run(ttl_wait_seconds=6)
    pipeline.close()

    kafka2 = InProcessKafkaQueue()
    streamer2 = SyntheticFleetStreamer(kafka2, topic=config.kafka_topic)
    pipeline2 = FleetStreamingPipeline(config, streamer2, kafka=kafka2)
    second = pipeline2.run(ttl_wait_seconds=6)
    pipeline2.close()

    assert first["warehouse_rows"] == second["warehouse_rows"]
    assert first["warehouse_rows"] >= 6
    assert first["dlq_files"] == 2
    assert second["dlq_files"] == 2


def test_reservation_hold_trigger_fires_once(tmp_path: Path) -> None:
    config = PipelineConfig.for_tests(tmp_path)
    kafka = InProcessKafkaQueue()
    pipeline = FleetStreamingPipeline(config, SyntheticFleetStreamer(kafka, topic=config.kafka_topic), kafka=kafka)
    summary = pipeline.run(ttl_wait_seconds=6)
    hold_triggers = [
        t for t in pipeline.triggers_fired if t["campaign_type"] == "Reservation Hold Expired"
    ]
    pipeline.close()
    assert summary["warehouse_rows"] >= 6
    assert len(hold_triggers) == 1


def test_sample_loads_and_processes(tmp_path: Path) -> None:
    sample = Path(__file__).resolve().parents[1] / "data" / "source" / "samples" / "fleet_rental_cdc.jsonl"
    assert sample.exists()
    reader = CdcJsonlReader(sample, max_rows=25)
    rows = list(reader)
    assert len(rows) == 25
    assert "rental_id" in rows[0]
    assert "event_type" in rows[0]

    config = PipelineConfig.for_tests(tmp_path)
    kafka = InProcessKafkaQueue()
    streamer = CdcJsonlStreamer(sample, kafka, topic=config.kafka_topic, max_rows=25)
    pipeline = FleetStreamingPipeline(config, streamer, kafka=kafka)
    summary = pipeline.run(max_events=25, ttl_wait_seconds=0)
    pipeline.close()
    assert summary["events_processed"] == 25
    assert summary["warehouse_rows"] == 25


def test_checkpoint_resume(tmp_path: Path) -> None:
    sample = Path(__file__).resolve().parents[1] / "data" / "source" / "samples" / "fleet_rental_cdc.jsonl"
    config = PipelineConfig.for_tests(tmp_path)
    kafka1 = InProcessKafkaQueue()
    streamer1 = CdcJsonlStreamer(sample, kafka1, topic=config.kafka_topic, max_rows=10)
    pipeline1 = FleetStreamingPipeline(config, streamer1, kafka=kafka1)
    pipeline1.run(max_events=10, ttl_wait_seconds=0)
    offset_after_first = streamer1.current_offset
    pipeline1.close()

    kafka2 = InProcessKafkaQueue()
    streamer2 = CdcJsonlStreamer(
        sample, kafka2, topic=config.kafka_topic, checkpoint_offset=offset_after_first, max_rows=10
    )
    pipeline2 = FleetStreamingPipeline(config, streamer2, kafka=kafka2)
    summary2 = pipeline2.run(max_events=10, ttl_wait_seconds=0)
    pipeline2.close()

    assert offset_after_first == 10
    assert summary2["events_processed"] == 10
    assert summary2["warehouse_rows"] == 20


def test_inventory_cache_and_conversion_kpi(tmp_path: Path) -> None:
    sample = Path(__file__).resolve().parents[1] / "data" / "source" / "samples" / "fleet_rental_cdc.jsonl"
    config = PipelineConfig.for_tests(tmp_path)
    kafka = InProcessKafkaQueue()
    streamer = CdcJsonlStreamer(sample, kafka, topic=config.kafka_topic, max_rows=50)
    pipeline = FleetStreamingPipeline(config, streamer, kafka=kafka)
    summary = pipeline.run(max_events=50, ttl_wait_seconds=0)
    pipeline.close()

    assert summary["inventory_snapshot"]
    gold = compute_gold_kpis(config.db_path, config.gold_table)
    assert gold["booking_conversion_pct"] > 0
    assert gold["fleet_utilization_pct"] > 0

    conn = duckdb.connect(str(tmp_path / "warehouse.db"), read_only=True)
    silver_rows = conn.execute("SELECT COUNT(*) FROM silver_rental_sessions").fetchone()[0]
    conn.close()
    assert silver_rows >= 1
