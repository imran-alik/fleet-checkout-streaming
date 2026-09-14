from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue

log = logging.getLogger(__name__)

FLEET_SAMPLE_ATTRIBUTION = (
    "Synthetic Debezium-style rental fleet CDC; generated for portfolio certification (no PII)."
)


class CdcJsonlReader:
    """Reads rental CDC events from JSON Lines file."""

    def __init__(self, jsonl_path: str | Path, *, start_offset: int = 0, max_rows: int | None = None):
        self.jsonl_path = Path(jsonl_path)
        self.start_offset = start_offset
        self.max_rows = max_rows
        if not self.jsonl_path.exists():
            raise FileNotFoundError(f"CDC sample not found: {self.jsonl_path}")

    def __iter__(self) -> Iterator[dict[str, Any]]:
        emitted = 0
        with self.jsonl_path.open("r", encoding="utf-8") as handle:
            for idx, line in enumerate(handle):
                if idx < self.start_offset:
                    continue
                if self.max_rows is not None and emitted >= self.max_rows:
                    break
                stripped = line.strip()
                if not stripped:
                    continue
                yield json.loads(stripped)
                emitted += 1

    def count_rows(self) -> int:
        count = 0
        with self.jsonl_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    count += 1
        return count


class CdcJsonlStreamer:
    """Checkpoint-aware CDC reader that publishes to in-process Kafka before consume."""

    def __init__(
        self,
        jsonl_path: str | Path,
        kafka: InProcessKafkaQueue,
        *,
        topic: str,
        checkpoint_offset: int = 0,
        max_rows: int | None = None,
    ):
        self.reader = CdcJsonlReader(jsonl_path, start_offset=checkpoint_offset, max_rows=max_rows)
        self.kafka = kafka
        self.topic = topic
        self._prefetched = False
        self.current_offset = checkpoint_offset
        self.source_path = str(Path(jsonl_path).resolve())

    def _prefetch_to_kafka(self) -> None:
        if self._prefetched:
            return
        for event in self.reader:
            self.kafka.publish(self.topic, event)
            self.current_offset += 1
        self._prefetched = True

    def get_next_event(self) -> dict[str, Any] | None:
        if not self._prefetched:
            self._prefetch_to_kafka()
        return self.kafka.consume(self.topic)
