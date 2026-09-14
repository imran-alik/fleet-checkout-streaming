#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CODEBASE = ROOT / "codebase"
sys.path.insert(0, str(CODEBASE))

from fleet_streaming.config.settings import PipelineConfig, _repo_root
from fleet_streaming.ingestion.cdc_jsonl import CdcJsonlStreamer
from fleet_streaming.ingestion.checkpoint import IngestCheckpoint
from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue
from fleet_streaming.ingestion.synthetic import SyntheticFleetStreamer
from fleet_streaming.orchestration.pipeline import FleetStreamingPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run fleet rental CDC streaming pipeline")
    parser.add_argument(
        "--source",
        choices=["synthetic", "sample"],
        default="sample",
        help="Event source: synthetic QA scenarios or committed JSONL sample",
    )
    parser.add_argument(
        "--jsonl-path",
        default=str(_repo_root() / "data" / "source" / "samples" / "fleet_rental_cdc.jsonl"),
        help="Path to fleet CDC JSONL sample",
    )
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N events")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint offset")
    parser.add_argument("--no-wait-ttl", action="store_true", help="Skip TTL wait at end")
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = build_parser().parse_args()
    config = PipelineConfig()
    config.ensure_dirs()
    kafka = InProcessKafkaQueue()

    if args.source == "synthetic":
        streamer = SyntheticFleetStreamer(kafka, topic=config.kafka_topic)
    else:
        jsonl_path = Path(args.jsonl_path)
        start_offset = 0
        if args.resume:
            checkpoint = IngestCheckpoint.load(config.checkpoint_path, source_path=str(jsonl_path.resolve()))
            start_offset = checkpoint.offset
        streamer = CdcJsonlStreamer(
            jsonl_path,
            kafka,
            topic=config.kafka_topic,
            checkpoint_offset=start_offset,
            max_rows=args.max_events,
        )

    pipeline = FleetStreamingPipeline(config, streamer, kafka=kafka)
    try:
        ttl_wait = 0.0 if args.no_wait_ttl else None
        summary = pipeline.run(max_events=args.max_events, ttl_wait_seconds=ttl_wait)
        print(json.dumps(summary, indent=2))
    finally:
        pipeline.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
