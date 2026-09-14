from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class DeadLetterQueue:
    def __init__(self, dlq_dir: str | Path) -> None:
        self.dlq_dir = Path(dlq_dir)
        self.dlq_dir.mkdir(parents=True, exist_ok=True)

    def route(self, raw_data: dict[str, Any], reason: str) -> Path:
        digest = hashlib.sha256(
            json.dumps({"raw": raw_data, "reason": reason}, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:12]
        filename = f"malformed_cdc_{digest}.json"
        filepath = self.dlq_dir / filename
        if filepath.exists():
            return filepath
        payload = {
            "quarantined_at": datetime.now(UTC).isoformat(),
            "failure_reason": reason,
            "raw_payload": raw_data,
        }
        filepath.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return filepath

    def count(self) -> int:
        return len(list(self.dlq_dir.glob("malformed_cdc_*.json")))
