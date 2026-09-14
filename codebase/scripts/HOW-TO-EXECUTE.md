# How to Execute — Fleet Checkout Streaming

Run all commands from **repository root** unless noted.

---

## 1. One-shot verification

```powershell
.\codebase\scripts\bootstrap.ps1
```

Runs: pip install → design gates → ruff → pytest → certification.

---

## 2. Run pipeline

```powershell
$env:PYTHONPATH="codebase"

# Committed 200-event sample (default)
py -3.12 codebase\scripts\run_pipeline.py --source sample

# Synthetic QA (DLQ + hold expiry)
py -3.12 codebase\scripts\run_pipeline.py --source synthetic

# Limit events / skip TTL wait
py -3.12 codebase\scripts\run_pipeline.py --source sample --max-events 50 --no-wait-ttl
```

---

## 3. Certification

```powershell
py -3.12 codebase\scripts\certify_public_run.py
```

Resets `data/sink/`, processes full sample, writes `data/evidence/run_summary_fleet_full_sample_*.json`.

Exit code `0` = all quality gates passed.

---

## 4. Tests and lint

```powershell
py -3.12 -m pytest -q
py -3.12 -m ruff check codebase tests
py -3.12 codebase\scripts\check_design_commit.py
```

---

## 5. Regenerate sample

```powershell
py -3.12 codebase\scripts\generate_sample.py
```

Writes 200 valid CDC lines to `data/source/samples/fleet_rental_cdc.jsonl`.

---

## 6. Analytics

After certification:

```powershell
duckdb data/sink/warehouse.db < analytics_query.sql
```

Or open `data/sink/warehouse.db` in DuckDB CLI/UI and run queries from `analytics_query.sql`.
