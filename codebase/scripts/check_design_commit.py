#!/usr/bin/env python
"""Static gates from docs/standards/design-commit.md (sections D, E file presence)."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

REQUIRED_FILES = [
    "README.md",
    "SOLUTION.md",
    "CHANGELOG.md",
    "data-dictionary.md",
    "docs/DESIGN.md",
    "docs/HANDOVER.md",
    "docs/gcp/GCP-SERVICES.md",
    "commands.txt",
    "docs/CONNECTIVITY.md",
    "docs/LIBRARIES.md",
    "codebase/requirements-runtime.txt",
    "codebase/scripts/bootstrap.ps1",
    "docs/standards/data-engineering-design-standards.md",
    "docs/standards/design-patterns.md",
    "docs/standards/design-commit.md",
    "analytics_query.sql",
    "codebase/scripts/HOW-TO-EXECUTE.md",
    "codebase/scripts/certify_public_run.py",
    "data/evidence/run_summary_index.json",
    "data/source/samples/fleet_rental_cdc.jsonl",
    "tests/test_pipeline.py",
    "tests/test_certification.py",
]

REQUIRED_MODULES = [
    "codebase/fleet_streaming/config/settings.py",
    "codebase/fleet_streaming/schemas/events.py",
    "codebase/fleet_streaming/idempotency.py",
    "codebase/fleet_streaming/ingestion/cdc_jsonl.py",
    "codebase/fleet_streaming/ingestion/checkpoint.py",
    "codebase/fleet_streaming/ingestion/kafka_queue.py",
    "codebase/fleet_streaming/state/redis_store.py",
    "codebase/fleet_streaming/warehouse/duckdb_loader.py",
    "codebase/fleet_streaming/quality/dlq.py",
    "codebase/fleet_streaming/orchestration/pipeline.py",
    "codebase/fleet_streaming/analytics/kpi.py",
    "codebase/fleet_streaming/evidence/writer.py",
]

DESIGN_HEADINGS = [
    "Verdicts",
    "Problem statement",
    "Ask register",
    "Sources and sinks",
    "Lineage",
    "Failure matrix",
    "Test plan",
    "Module map",
    "Document control",
]

ASK_PATTERN = re.compile(r"\*\*A[1-9]\*\*")


def main() -> int:
    failures: list[str] = []

    for rel in REQUIRED_FILES + REQUIRED_MODULES:
        if not (ROOT / rel).exists():
            failures.append(f"Missing required path: {rel}")

    design = (ROOT / "docs" / "DESIGN.md").read_text(encoding="utf-8")
    for heading in DESIGN_HEADINGS:
        if heading.lower() not in design.lower():
            failures.append(f"DESIGN.md missing section keyword: {heading}")

    solution = (ROOT / "SOLUTION.md").read_text(encoding="utf-8")
    design_asks = set(re.findall(r"\*\*A(\d+)\*\*", design))
    solution_asks = set(re.findall(r"## A(\d+)", solution))
    if design_asks and design_asks != solution_asks:
        failures.append(f"Ask ID drift DESIGN {sorted(design_asks)} vs SOLUTION {sorted(solution_asks)}")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for token in (
        "docs/DESIGN.md",
        "docs/HANDOVER.md",
        "docs/gcp/GCP-SERVICES.md",
        "SOLUTION.md",
        "Overview",
        "Architecture",
        "commands.txt",
        "analytics_query.sql",
    ):
        if token not in readme:
            failures.append(f"README.md missing link/section: {token}")

    libraries_doc = (ROOT / "docs" / "LIBRARIES.md").read_text(encoding="utf-8")
    for req_file in ("codebase/requirements.txt", "codebase/requirements-runtime.txt"):
        req_path = ROOT / req_file
        if not req_path.exists():
            continue
        for line in req_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("-r"):
                continue
            package = stripped.split("==")[0].split(">=")[0].split("[")[0].strip()
            if package.lower() not in libraries_doc.lower():
                failures.append(f"Package '{package}' in {req_file} not documented in docs/LIBRARIES.md")

    if failures:
        print("DESIGN-COMMIT STATIC CHECK FAILED:", file=sys.stderr)
        for item in failures:
            print(f"  - {item}", file=sys.stderr)
        return 1

    print("Design-commit static checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
