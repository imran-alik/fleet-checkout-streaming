from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def write_run_summary(
    *,
    evidence_dir: str | Path,
    run_name: str,
    payload: dict[str, Any],
) -> Path:
    target_dir = Path(evidence_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = target_dir / f"run_summary_{run_name}_{stamp}.json"
    envelope = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "run_name": run_name,
        **payload,
    }
    path.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
    _update_index(target_dir, run_name, path.name, envelope)
    return path


def _update_index(evidence_dir: Path, run_name: str, filename: str, summary: dict[str, Any]) -> None:
    index_path = evidence_dir / "run_summary_index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text(encoding="utf-8"))
    else:
        index = {"runs": []}
    index["runs"].append(
        {
            "run_name": run_name,
            "artifact": filename,
            "generated_at_utc": summary["generated_at_utc"],
            "source": summary.get("source"),
            "events_processed": summary.get("pipeline", {}).get("events_processed"),
            "bronze_rows": summary.get("warehouse_kpis", {}).get("bronze_rows"),
            "fleet_utilization_pct": summary.get("gold_kpis", {}).get("fleet_utilization_pct"),
            "booking_conversion_pct": summary.get("gold_kpis", {}).get("booking_conversion_pct"),
        }
    )
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")
