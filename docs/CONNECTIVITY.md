# Connectivity Map — Code ↔ Doc ↔ Test ↔ Evidence

> Traceability matrix for the fleet checkout streaming case study.  
> Required by [data-engineering-design-standards.md](standards/data-engineering-design-standards.md) §2.

---

## 1. Ask traceability

| Ask | Business need | Module(s) | Test(s) | Doc section | Evidence field |
|---|---|---|---|---|---|
| **A1** | Ingest rental CDC | `ingestion/cdc_jsonl.py`, `warehouse/duckdb_loader.py` | `test_sample_loads_and_processes`, `test_certification` | DESIGN §6 A1, SOLUTION A1 | `pipeline.events_processed`, `warehouse_kpis.bronze_rows` |
| **A2** | Schema validation + DLQ | `schemas/events.py`, `quality/dlq.py` | `test_synthetic_pipeline_idempotent_on_replay` | DESIGN §6 A2 | `pipeline.dlq_files` |
| **A3** | Live inventory + hold expiry | `state/redis_store.py`, `orchestration/pipeline.py` | `test_reservation_hold_trigger_fires_once` | DESIGN §6 A3 | `triggers.by_campaign_type` |
| **A4** | Medallion KPIs | `warehouse/duckdb_loader.py`, `analytics/kpi.py` | `test_inventory_cache_and_conversion_kpi` | DESIGN §6 A4 | `gold_kpis` |
| **A5** | Idempotent replay + resume | `idempotency.py`, `ingestion/checkpoint.py` | `test_synthetic_pipeline_idempotent_on_replay`, `test_checkpoint_resume` | DESIGN §6 A5 | `quality_gates.idempotent_replay_row_count_stable` |
| **A6** | Reviewer evidence | `scripts/certify_public_run.py`, `evidence/writer.py` | `test_certification` | DESIGN §6 A6 | `data/evidence/run_summary_index.json` |

---

## 2. File ↔ purpose

| Path | Role | Consumed by |
|---|---|---|
| `README.md` | Overview, architecture, metrics | Humans first |
| `docs/HANDOVER.md` | Handover checklist | Reviewers |
| `docs/gcp/GCP-SERVICES.md` | GCP production mapping | Cloud migration |
| `commands.txt` | CLI reference | Quick start |
| `analytics_query.sql` | KPI SQL | Post-certify analytics |
| `docs/DESIGN.md` | Authoritative design | Engineers |
| `SOLUTION.md` | Ask-by-ask proof | Hiring reviewers |
| `data-dictionary.md` | Column definitions | DESIGN §5 |
| `data/source/samples/fleet_rental_cdc.jsonl` | Committed proof input | certification, tests |
| `data/evidence/run_summary_*.json` | Certified output | README, reviewers |
| `tests/test_pipeline.py` | Unit + integration | pytest |
| `tests/test_certification.py` | Certification subprocess | pytest |
