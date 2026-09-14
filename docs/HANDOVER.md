# Handover — Fleet Checkout Streaming Pipeline

Checklist for engineers or reviewers taking over this repo.

---

## 1. What this pipeline does

Ingests rental fleet CDC events → validates → updates live vehicle inventory in Redis → loads bronze/silver/gold medallion → fires hold-expiry triggers → writes certification evidence.

**Proven locally** on 200-event committed sample. **GCP services** documented, not wired — see [gcp/GCP-SERVICES.md](gcp/GCP-SERVICES.md).

---

## 2. Verify in 5 minutes

```powershell
cd fleet-checkout-streaming
py -3.12 -m pip install -r codebase\requirements.txt
$env:PYTHONPATH="codebase"
py -3.12 codebase\scripts\certify_public_run.py
py -3.12 -m pytest -q
```

| Pass criteria | Expected |
|---|---|
| Certify exit code | `0` |
| Bronze rows | 200 |
| DLQ (sample) | 0 |
| pytest | 7 / 7 green |

Evidence: latest file under `data/evidence/` indexed by `run_summary_index.json`.

---

## 3. Key modules

| Concern | Path |
|---|---|
| Config | `codebase/fleet_streaming/config/settings.py` |
| Schema | `codebase/fleet_streaming/schemas/events.py` |
| Idempotency key | `codebase/fleet_streaming/idempotency.py` |
| Orchestrator | `codebase/fleet_streaming/orchestration/pipeline.py` |
| Certification | `codebase/scripts/certify_public_run.py` |

Traceability: [CONNECTIVITY.md](CONNECTIVITY.md)

---

## 4. Design & ops docs

| Document | When to read |
|---|---|
| [DESIGN.md](DESIGN.md) | Architecture, verdicts, failure matrix |
| [SOLUTION.md](../SOLUTION.md) | Business asks A1–A6 |
| [data-dictionary.md](../data-dictionary.md) | Column contracts |
| [gcp/GCP-SERVICES.md](gcp/GCP-SERVICES.md) | GCP production mapping |
| [HOW-TO-EXECUTE.md](../codebase/scripts/HOW-TO-EXECUTE.md) | All CLI options |

---

## 5. Runtime outputs (gitignored)

| Path | Contents |
|---|---|
| `data/sink/warehouse.db` | DuckDB medallion tables |
| `data/sink/quarantine_dlq/` | DLQ JSON files |
| `data/sink/checkpoints/` | Ingest offset |

Delete `data/sink/` for a clean re-run (certification script does this automatically).

---

## 6. Common issues

| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: fleet_streaming` | Set `$env:PYTHONPATH="codebase"` |
| Certification gate failure | Run `generate_sample.py`; check sample has 200 lines |
| Hold trigger test flaky | Ensure `reservation_hold_ttl_seconds` wait in test (6s) |
| DuckDB lock error | Close other DuckDB connections to `warehouse.db` |
