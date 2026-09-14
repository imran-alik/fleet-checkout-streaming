from __future__ import annotations

import logging
from collections import deque
from typing import Any

log = logging.getLogger(__name__)


class InProcessKafkaQueue:
    """In-process Kafka topic emulator (deque per topic). Production: managed Kafka / Pub/Sub."""

    def __init__(self) -> None:
        self._topics: dict[str, deque[dict[str, Any]]] = {}

    def publish(self, topic: str, event: dict[str, Any]) -> None:
        if topic not in self._topics:
            self._topics[topic] = deque()
        self._topics[topic].append(event)
        log.debug("[KAFKA] publish topic=%s rental_id=%s", topic, event.get("rental_id"))

    def consume(self, topic: str) -> dict[str, Any] | None:
        queue = self._topics.get(topic)
        if not queue:
            return None
        event = queue.popleft()
        log.debug("[KAFKA] consume topic=%s rental_id=%s", topic, event.get("rental_id"))
        return event

    def depth(self, topic: str) -> int:
        return len(self._topics.get(topic, []))
