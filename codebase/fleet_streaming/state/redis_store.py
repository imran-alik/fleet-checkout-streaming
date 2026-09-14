from __future__ import annotations

import heapq
import logging
import time
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


@dataclass(order=True)
class _ExpiryEntry:
    expiry_ts: float
    key: str


class LocalRedisStore:
    """In-process Redis emulator for live vehicle inventory with TTL holds."""

    def __init__(self, default_ttl_seconds: int) -> None:
        self.default_ttl_seconds = default_ttl_seconds
        self._store: dict[str, dict[str, Any]] = {}
        self._expiry_heap: list[_ExpiryEntry] = []
        self._triggered: set[str] = set()

    def set_with_ttl(self, key: str, value: dict[str, Any], ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds or self.default_ttl_seconds
        self._store[key] = value
        heapq.heappush(self._expiry_heap, _ExpiryEntry(time.time() + ttl, key))
        log.info("[REDIS] Set '%s' TTL=%ss status=%s", key, ttl, value.get("status"))

    def set_persistent(self, key: str, value: dict[str, Any]) -> None:
        self._store[key] = value
        log.info("[REDIS] Set '%s' status=%s", key, value.get("status"))

    def get(self, key: str) -> dict[str, Any] | None:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        if key in self._store:
            del self._store[key]
            log.info("[REDIS] Deleted '%s'", key)

    def mark_triggered(self, trigger_key: str) -> bool:
        if trigger_key in self._triggered:
            return False
        self._triggered.add(trigger_key)
        return True

    def inventory_snapshot(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for key, value in self._store.items():
            if not key.startswith("inventory:"):
                continue
            status = str(value.get("status", "unknown"))
            counts[status] = counts.get(status, 0) + 1
        return counts

    def pop_expired(self) -> list[tuple[str, dict[str, Any]]]:
        now = time.time()
        expired: list[tuple[str, dict[str, Any]]] = []
        while self._expiry_heap and self._expiry_heap[0].expiry_ts <= now:
            entry = heapq.heappop(self._expiry_heap)
            if entry.key not in self._store:
                continue
            expired.append((entry.key, self._store.pop(entry.key)))
        return expired
