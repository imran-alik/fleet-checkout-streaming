from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class PipelineConfig:
    db_path: str = field(default_factory=lambda: str(_repo_root() / "data" / "sink" / "warehouse.db"))
    bronze_table: str = "bronze_rental_cdc"
    silver_table: str = "silver_rental_sessions"
    gold_table: str = "gold_fleet_kpis"
    dlq_dir: str = field(default_factory=lambda: str(_repo_root() / "data" / "sink" / "quarantine_dlq"))
    checkpoint_path: str = field(
        default_factory=lambda: str(_repo_root() / "data" / "sink" / "checkpoints" / "ingest.offset.json")
    )
    kafka_topic: str = "rental.cdc.v1"
    reservation_hold_ttl_seconds: int = 5
    stream_delay_seconds: float = 0.0
    duckdb_batch_size: int = 50

    def ensure_dirs(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_path).parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def for_tests(cls, tmp_path: Path) -> PipelineConfig:
        return cls(
            db_path=str(tmp_path / "warehouse.db"),
            dlq_dir=str(tmp_path / "quarantine_dlq"),
            checkpoint_path=str(tmp_path / "checkpoints" / "ingest.offset.json"),
            stream_delay_seconds=0.0,
        )
