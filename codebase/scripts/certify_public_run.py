#!/usr/bin/env python
"""Certified sample run: 200 CDC events, medallion KPIs, idempotency replay check."""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CODEBASE = ROOT / "codebase"
sys.path.insert(0, str(CODEBASE))

from fleet_streaming.analytics.kpi import compute_bronze_kpis, compute_gold_kpis, summarize_triggers
from fleet_streaming.config.settings import PipelineConfig, _repo_root
from fleet_streaming.evidence.writer import write_run_summary
from fleet_streaming.ingestion.cdc_jsonl import CdcJsonlReader, CdcJsonlStreamer
from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue
from fleet_streaming.orchestration.pipeline import FleetStreamingPipeline


def _reset_sink(config: PipelineConfig) -> None:
    sink = Path(config.db_path).parent
    if sink.exists():
        shutil.rmtree(sink)
    config.ensure_dirs()


def main() -> int:
    config = PipelineConfig()
    jsonl_path = _repo_root() / "data" / "source" / "samples" / "fleet_rental_cdc.jsonl"
    evidence_dir = _repo_root() / "data" / "evidence"

    source_profile = CdcJsonlReader(jsonl_path)
    source_rows = source_profile.count_rows()

    _reset_sink(config)
    started = time.perf_counter()

    kafka = InProcessKafkaQueue()
    streamer = CdcJsonlStreamer(jsonl_path, kafka, topic=config.kafka_topic)
    pipeline = FleetStreamingPipeline(config, streamer, kafka=kafka)
    run_summary = pipeline.run(ttl_wait_seconds=config.reservation_hold_ttl_seconds + 1)
    trigger_summary = summarize_triggers(pipeline.triggers_fired)
    warehouse_kpis = compute_bronze_kpis(conn=pipeline.warehouse.conn, bronze_table=config.bronze_table)
    gold_kpis = compute_gold_kpis(conn=pipeline.warehouse.conn, gold_table=config.gold_table)
    pipeline.close()

    kafka_replay = InProcessKafkaQueue()
    streamer_replay = CdcJsonlStreamer(jsonl_path, kafka_replay, topic=config.kafka_topic)
    pipeline_replay = FleetStreamingPipeline(config, streamer_replay, kafka=kafka_replay)
    replay_summary = pipeline_replay.run(ttl_wait_seconds=0)
    rows_after_replay = pipeline_replay.warehouse.count()
    pipeline_replay.close()

    elapsed = round(time.perf_counter() - started, 3)
    certification = {
        "problem": "Real-time fleet rental checkout streaming with live inventory and medallion KPIs",
        "source": {
            "name": "Synthetic Debezium-style rental fleet CDC",
            "file": str(jsonl_path.relative_to(_repo_root())).replace("\\", "/"),
            "attribution": "Portfolio-generated sample; no PII or production credentials",
            "source_rows_in_file": source_rows,
        },
        "pipeline": run_summary,
        "warehouse_kpis": warehouse_kpis,
        "gold_kpis": gold_kpis,
        "triggers": trigger_summary,
        "quality_gates": {
            "all_source_rows_ingested": run_summary["events_processed"] == source_rows,
            "bronze_rows_match_run_summary": warehouse_kpis["bronze_rows"] == run_summary["warehouse_rows"],
            "bronze_rows_match_processed": warehouse_kpis["bronze_rows"] == run_summary["events_processed"],
            "dlq_count_zero_on_sample": run_summary["dlq_files"] == 0,
            "idempotent_replay_row_count_stable": rows_after_replay == warehouse_kpis["bronze_rows"],
            "fleet_utilization_kpi_computed": gold_kpis.get("fleet_utilization_pct", 0) > 0,
            "booking_conversion_kpi_computed": gold_kpis.get("booking_conversion_pct", 0) > 0,
            "silver_layer_built": run_summary.get("silver_rows", 0) >= warehouse_kpis["distinct_rentals"],
        },
        "replay": replay_summary,
        "timing_seconds": elapsed,
    }

    artifact = write_run_summary(
        evidence_dir=evidence_dir,
        run_name="fleet_full_sample",
        payload=certification,
    )
    print(json.dumps({"artifact": str(artifact), **certification}, indent=2))

    failed = [k for k, v in certification["quality_gates"].items() if not v]
    if failed:
        print(f"QUALITY GATE FAILURES: {failed}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
