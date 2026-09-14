# Solution — Fleet Checkout Streaming Pipeline (Ask by Ask)

> Maps business asks to code modules and **expected measurable outputs**.  
> Traceability: [`docs/CONNECTIVITY.md`](docs/CONNECTIVITY.md) · Standards: [`docs/standards/data-engineering-design-standards.md`](docs/standards/data-engineering-design-standards.md)

Certified run index: [`data/evidence/run_summary_index.json`](data/evidence/run_summary_index.json)

---

## Business context

**Problem:** Car rental operators need live fleet inventory and checkout conversion metrics while CDC events stream from regional rental databases. Batch nightly loads miss hold expirations and utilization swings.

## Net outcome (certified sample run)

| Deliverable | Measured result |
|---|---|
| Bronze ingest | 200 / 200 CDC events |
| Medallion | Silver sessions + gold KPIs built |
| Fleet utilization | Computed in `gold_fleet_kpis` |
| Booking conversion | Computed in `gold_fleet_kpis` |
| Data quality | 0 DLQ files on committed sample |
| Idempotent replay | 200 bronze rows stable |
| Test coverage | 7 / 7 pytest pass + certification subprocess |

---

## A1 — Ingest rental CDC into bronze

| | |
|---|---|
| **ASK** | Stream Debezium-style rental CDC into governed bronze |
| **SOLUTION** | `CdcJsonlReader` parses JSONL; in-process Kafka queue; orchestrator validates and UPSERTs |
| **CODE** | `ingestion/cdc_jsonl.py`, `ingestion/kafka_queue.py`, `warehouse/duckdb_loader.py` |
| **EXPECTED OUTPUT** | Certification: `events_processed = 200`, `warehouse_kpis.bronze_rows = 200` |

---

## A2 — Schema validation and DLQ

| | |
|---|---|
| **ASK** | Invalid CDC rows must not break the stream |
| **SOLUTION** | Pydantic contract + negative-rate DQ; hash-deduped DLQ JSON files |
| **CODE** | `schemas/events.py`, `quality/dlq.py` |
| **EXPECTED OUTPUT** | Sample run: `dlq_files = 0`; Synthetic run: `dlq_files = 2` |

---

## A3 — Live inventory with reservation hold expiry

| | |
|---|---|
| **ASK** | Track vehicle availability; detect expired holds for reallocation |
| **SOLUTION** | Redis key `inventory:{vehicle_id}` with TTL on reservation; expiry fires deduped trigger |
| **CODE** | `state/redis_store.py`, `orchestration/pipeline.py` |
| **EXPECTED OUTPUT** | Synthetic run: 1 `Reservation Hold Expired` trigger |

---

## A4 — Medallion silver/gold KPIs

| | |
|---|---|
| **ASK** | Booking conversion and fleet utilization for ops dashboards |
| **SOLUTION** | Silver session aggregation + gold metric table refresh after bronze flush |
| **CODE** | `warehouse/duckdb_loader.py`, `analytics/kpi.py` |
| **EXPECTED OUTPUT** | `gold_kpis.fleet_utilization_pct > 0`, `gold_kpis.booking_conversion_pct > 0` |

---

## A5 — Idempotent replay and resume

| | |
|---|---|
| **ASK** | Reprocessing must not duplicate bronze rows; runs must resume after interruption |
| **SOLUTION** | Deterministic `cdc_id` hash + UPSERT; checkpoint offset JSON per source file |
| **CODE** | `idempotency.py`, `ingestion/checkpoint.py` |
| **EXPECTED OUTPUT** | `quality_gates.idempotent_replay_row_count_stable = true`; `test_checkpoint_resume` |

---

## A6 — Reviewer evidence artifact

| | |
|---|---|
| **ASK** | Ship auditable certification JSON with quality gates |
| **SOLUTION** | `certify_public_run.py` runs full sample, checks gates, writes indexed evidence |
| **CODE** | `scripts/certify_public_run.py`, `evidence/writer.py` |
| **EXPECTED OUTPUT** | `data/evidence/run_summary_fleet_full_sample_*.json` with all gates true |
