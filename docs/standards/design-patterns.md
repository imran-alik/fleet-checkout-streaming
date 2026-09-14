# Data Engineering Design Patterns

> Portfolio and pipeline patterns distilled from production-oriented repos — not tutorial walkthroughs.  
> Reference implementations: [vishal-bulbule](https://github.com/vishal-bulbule) (GCP DE projects), [uber-data-engineering-mage-project](https://github.com/darshilparmar/uber-data-engineering-mage-project) (ETL + analytics SQL), [Retail_Analysis_Redshift](https://github.com/ansamAY/Retail_Analysis_Redshift) (layered warehouse models).

Use with [`data-engineering-design-standards.md`](./data-engineering-design-standards.md) and [`design-commit.md`](./design-commit.md).

---

## 1. Portfolio README pattern (GitHub front door)

A hiring reviewer spends **≤60 seconds** on the repo landing page. Structure like a featured project on an engineer profile — not a course syllabus.

### 1.1 Required above-the-fold content

| Block | Pattern source | What to show |
|---|---|---|
| **Title + domain** | [vishal-bulbule featured projects](https://github.com/vishal-bulbule) | `{Domain} \| {Pattern} on {Cloud}` — e.g. *Real-Time Checkout Telemetry \| Stream Processing on GCP* |
| **Tech stack table** | [Retail_Analysis_Redshift](https://github.com/ansamAY/Retail_Analysis_Redshift) | Layer × tool × production analogue |
| **Pipeline outcomes** | This repo's certification JSON | **Committed stats** — row counts, funnel metrics, trigger counts, at-risk revenue, DLQ rate, replay stability |
| **Architecture** | [uber-data-engineering-mage-project](https://github.com/darshilparmar/uber-data-engineering-mage-project) | Diagram (Mermaid or image) with **module paths**, not generic boxes |
| **Data model overview** | Retail `dim_` / `fact_` / marts | Bronze / state / evidence / DLQ layers with grain and keys |
| **Pipeline scripts** | Uber extract/load/transform links | Direct links to ingestion, orchestration, warehouse, analytics modules |
| **Getting started** | Retail numbered steps | Install → certify → test — with **pass criteria**, not marketing copy |
| **Design docs** | Internal standard | Links to `DESIGN.md`, `SOLUTION.md`, evidence index |

### 1.2 Language — engineer vs tutorial (BTS)

| Avoid (tutorial / BTS tone) | Use (engineer tone) |
|---|---|
| plug and play | reproducible certification harness |
| simple / easy | local emulator of `{production service}` |
| just run / we'll leverage | execute / implements |
| layman view / plain language only | business context + measurable outcomes |
| dive into the world of | processes / ingests / emits |
| toy demo | local proof with committed evidence |
| magic / automatically handles | deterministic UPSERT on `{key}` |

Commits and CHANGELOG entries follow the same rule: **imperative subject + measurable outcome in body**.

---

## 2. ETL script separation (Uber Mage pattern)

Separate concerns into named pipeline stages with **one module per stage**:

| Stage | Responsibility | This repo module | Production analogue |
|---|---|---|---|
| **Extract** | Read source, normalize schema | `ingestion/public_clickstream.py` | Pub/Sub subscriber, Kafka consumer |
| **Transform** | Validate, stateful rules, triggers | `orchestration/pipeline.py` | Dataflow / Flink job |
| **Load** | Idempotent sink writes | `warehouse/duckdb_loader.py` | BigQuery MERGE |
| **Analytics** | Post-run KPI SQL | `analytics/kpi.py`, `analytics_query.sql` | Looker / dbt marts |
| **Certify** | Evidence + quality gates | `scripts/certify_public_run.py` | dbt test + observability export |

Orchestrator wires adapters; **business rules stay in orchestrator**, I/O in stage modules.

---

## 3. Layered data model (Retail / dbt pattern)

Name layers by **warehouse convention**, even when local:

| Layer | Prefix / name | Grain | Idempotency key |
|---|---|---|---|
| **Source** | committed CSV / API | 1 row = 1 interaction | composite fields → `event_id` hash |
| **Bronze** | `bronze_*` | 1 row = 1 validated event | `event_id` UPSERT |
| **State** | Redis keys | 1 key = 1 session flag | TTL + dedupe prefix `trigger:` |
| **Evidence** | `run_summary_*.json` | 1 file = 1 certified run | UTC timestamp |
| **DLQ** | `quarantine_dlq/*.json` | 1 file = 1 poison message | content hash |
| **Marts** (future) | `mart_*` / `dim_*` | session / campaign aggregates | dbt incremental |

Document the model in README **before** folder tree. Reviewers care about grain and keys first.

---

## 4. Evidence-driven outcomes (production proof pattern)

Every portfolio pipeline must answer: **what changed in the data after this run?**

### 4.1 Committed outcome artifact

Certification writes JSON with:

- `pipeline.events_processed`, `warehouse_kpis.*`, `triggers.*`
- `quality_gates` booleans (all must be `true` to commit)
- `replay` block proving idempotency (same row count, zero duplicate triggers)

Index file `data/evidence/run_summary_index.json` points to latest artifact.

### 4.2 README outcome table (mandatory)

Surface **net activity outcome** on README — not buried in `data/evidence/`:

| Metric | Certified value | Source field |
|---|---|---|
| Ingest fidelity | 400 / 400 (100%) | `quality_gates.all_source_rows_ingested` |
| Bronze rows | 400 | `warehouse_kpis.bronze_rows` |
| Sessions / users | 165 / 162 | `warehouse_kpis.distinct_*` |
| Cart → no purchase sessions | 9 | `warehouse_kpis.sessions_cart_without_purchase` |
| Recovery triggers emitted | 10 | `triggers.total_triggers` |
| At-risk revenue surfaced | $4,722.74 | sum of `triggers[].data.lost_revenue` |
| DLQ on public sample | 0 | `pipeline.dlq_files` |
| Replay stability | row count unchanged; 0 re-triggers | `replay.*` |
| Test suite | 6 / 6 pass | `pytest` |
| Run duration | ~8s | `timing_seconds` |

Refresh this table whenever certification is re-run and gates change.

### 4.3 Analytics SQL (Uber pattern)

Ship `analytics_query.sql` with funnel / KPI queries reviewers can run against bronze — mirrors [uber analytics_query.sql](https://github.com/darshilparmar/uber-data-engineering-mage-project/blob/main/analytics_query.sql).

---

## 5. Stateful stream processing patterns

| Pattern | Implementation | Proves |
|---|---|---|
| **Session TTL** | `cart_abandon:{session}` with expiry heap | Cart abandonment without purchase |
| **Trigger dedupe** | `trigger:{campaign}:{session}` | At-most-once campaign side effect |
| **Counter threshold** | `payment_friction:{session}` | N failures → one assist trigger |
| **Checkpoint** | offset JSON per source file | Crash resume |
| **Deterministic ID** | SHA-256 of canonical event fields | At-least-once → exactly-once bronze |

---

## 6. Quality and resilience patterns

| Pattern | When | Module |
|---|---|---|
| **Schema boundary validation** | Every ingest row | `schemas/events.py` (Pydantic) |
| **DLQ quarantine** | Invalid row; stream continues | `quality/dlq.py` |
| **Fail fast** | Missing source at startup | `public_clickstream.py` |
| **UPSERT idempotency** | Duplicate delivery / replay | `duckdb_loader.py` |
| **Synthetic path** | Behavior public data cannot show | `ingestion/synthetic.py` |

Label each pattern **Proven (local)** vs **Documented (scale-out)** in DESIGN verdicts.

---

## 7. GitHub profile alignment ([vishal-bulbule](https://github.com/vishal-bulbule))

| Profile element | Repo equivalent |
|---|---|
| Featured projects table (name · description · stack) | README title + tech stack + one-line outcome |
| Tech arsenal sections | README tech stack grouped by Ingestion / Processing / Warehouse / Quality |
| Build in public / real deployments | Committed evidence JSON + honest roadmap for GCP |
| Credentials / proof | Certification gates + pytest, not unsubstantiated claims |

Pin this repo with a description that states **measurable outcome** (e.g. *400-row public clickstream → 10 recovery triggers, $4.7K at-risk revenue, 0 DLQ*).

---

## 8. Commit message pattern

```
feat(ingestion): add checkpoint resume for public CSV stream

Public sample certification still passes 400/400 with idempotent replay.
Evidence index updated — triggers_fired unchanged on replay run.

Refs: A5
```

| Part | Rule |
|---|---|
| Subject | Imperative, ≤72 chars, scope optional |
| Body | **Why** + measurable outcome + ask ID |
| Avoid | "simple fix", "plug and play", "updated stuff" |

---

## 9. Pre-merge checklist additions

Beyond [`design-commit.md`](./design-commit.md):

- [ ] README **Pipeline Outcomes** table matches latest `run_summary_*.json`
- [ ] No tutorial/BTS language in user-facing docs (§1.2)
- [ ] `analytics_query.sql` present and referenced
- [ ] Script modules linked in README (Uber pattern)
- [ ] Data model layers documented (Retail pattern)

---

## 10. Reference repos

| Repo | Take |
|---|---|
| [vishal-bulbule](https://github.com/vishal-bulbule) | Profile layout, production-grade framing, featured project tables |
| [uber-data-engineering-mage-project](https://github.com/darshilparmar/uber-data-engineering-mage-project) | Architecture image, ETL script links, `analytics_query.sql`, data dictionary link |
| [Retail_Analysis_Redshift](https://github.com/ansamAY/Retail_Analysis_Redshift) | Tech stack, dim/fact/mart naming, numbered getting started |
| [danielbeach/data-engineering-practice](https://github.com/danielbeach/data-engineering-practice) | Progressive exercise structure, Docker, tests per exercise |
| [vishal-bulbule/etl-pipeline-datafusion-airflow](https://github.com/vishal-bulbule/etl-pipeline-datafusion-airflow) | End-to-end GCP ETL with Composer |
