from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class IngestCheckpoint:
    source_path: str
    offset: int = 0

    @classmethod
    def load(cls, path: str | Path, *, source_path: str) -> IngestCheckpoint:
        checkpoint_file = Path(path)
        if not checkpoint_file.exists():
            return cls(source_path=source_path, offset=0)
        data = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        if data.get("source_path") != source_path:
            return cls(source_path=source_path, offset=0)
        return cls(source_path=source_path, offset=int(data.get("offset", 0)))

    def save(self, path: str | Path) -> None:
        checkpoint_file = Path(path)
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_file.write_text(json.dumps(asdict(self), indent=2), encoding="utf-8")
