# Data Engineering Design Standards

> Canonical standard for portfolio-grade DE case studies, pipelines, and repos.  
> Synthesized from production-oriented portfolios ([vishal-bulbule](https://github.com/vishal-bulbule)), ETL case studies ([uber-data-engineering-mage-project](https://github.com/darshilparmar/uber-data-engineering-mage-project)), warehouse layering ([Retail_Analysis_Redshift](https://github.com/ansamAY/Retail_Analysis_Redshift)), practice repos ([danielbeach/data-engineering-practice](https://github.com/danielbeach/data-engineering-practice)), and curated lists ([igorbarinov/awesome-data-engineering](http://github.com/igorbarinov/awesome-data-engineering)).

Companion pattern catalog: [`design-patterns.md`](./design-patterns.md).

---

## 1. Purpose

A data engineering deliverable is **not** a script dump. It is a **reproducible system** that:

1. States a business problem a non-engineer understands
2. Maps every requirement to code, tests, and measurable output
3. Survives replay, partial failure, and reviewer scrutiny
4. Documents what is proven locally vs what is scale-out design only

Use this document when authoring `README.md`, `docs/DESIGN.md`, `SOLUTION.md`, and evidence artifacts.

---

## 2. Required repository artifacts

| Artifact | Required | Purpose |
|---|---|---|
| `README.md` | **Yes** | Tech stack, pipeline outcomes, architecture, data model, getting started |
| `docs/DESIGN.md` | **Yes** | Authoritative design contract (source of truth) |
| `SOLUTION.md` | **Yes** | ASK → SOLUTION → CODE → EXPECTED OUTPUT per requirement |
| `data-dictionary.md` | **Yes** | Source + sink schemas with grain and keys |
| `CHANGELOG.md` | **Yes** | [Keep a Changelog](https://keepachangelog.com/) format |
| `docs/CONNECTIVITY.md` | **Yes** | Code ↔ doc ↔ file ↔ test traceability matrix |
| `docs/standards/design-commit.md` | **Yes** | Pre-merge gate checklist |
| `codebase/scripts/HOW-TO-EXECUTE.md` | **Yes** | Copy-paste runbook with pass criteria |
| `data/evidence/` | **When provable** | Committed certification JSON for public-data proof |
| `tests/` | **Yes** | pytest unit + integration + certification subprocess |
| `env/.env.example` | **Yes** | No secrets in repo; config via env/dataclass |
| `docs/LIBRARIES.md` | **Yes** | Dependency catalog: PyPI source, docs, pins, upgrade procedure |
| `docs/standards/design-patterns.md` | **Yes** | Portfolio layout, ETL separation, outcome stats, language standards |
| `analytics_query.sql` | **Recommended** | Funnel KPI SQL reviewers can execute against bronze |
| `codebase/requirements-runtime.txt` | **Yes** | Minimal runtime-only install for certification |
| `codebase/scripts/bootstrap.ps1` or `bootstrap.sh` | **Recommended** | Local verification harness (lint + tests + certify) |
| `.github/workflows/ci.yml` | **Recommended** | pytest + lint on push/PR |

Optional but portfolio-strong: `docker-compose.yml`, `Makefile`/`justfile`, `CONTRIBUTING.md`.

---

## 3. README structure (explanatory design entry point)

README is the **front door**. A reviewer should never hunt for the problem statement.

### 3.1 Mandatory sections

1. **Title + domain** — `{Use case} | {Pattern} on {Cloud}` (portfolio front door)
2. **Tech stack table** — layer × local tool × production analogue
3. **Pipeline outcomes** — **committed stats from certification JSON** (ingest fidelity, funnel, triggers, at-risk revenue, DLQ, replay stability, tests, duration)
4. **Architecture diagram** — Mermaid with **module paths**, not generic boxes
5. **Data model overview** — bronze / state / evidence / DLQ layers with grain and keys
6. **Pipeline scripts** — linked extract / transform / load / analytics / certify modules
7. **Getting started** — numbered install → certify → test with pass criteria
8. **Dataset attribution** — source URL, dictionary, committed sample stats
9. **Project structure** — annotated tree
10. **Production roadmap** — honest Proven vs Documented vs Planned
11. **Design deep-dive links** — `DESIGN.md`, `SOLUTION.md`, `CONNECTIVITY.md`, evidence index
12. **References** — portfolio repos that informed structure

See [`design-patterns.md`](./design-patterns.md) §1 for layout examples.

### 3.2 README must not

- Bury pipeline outcomes inside `data/evidence/` without a summary table on README
- Use tutorial/BTS language (“plug and play”, “simple”, “dive into”, “layman only”)
- Claim cloud scale as “implemented” when only documented
- Omit how to verify success (exit codes, row counts, gate names)
- Lead with folder tree before tech stack and outcomes

---

## 4. Design document (`docs/DESIGN.md`) contract

Every design doc MUST include the sections below. Missing any section = **not merge-ready**.

| # | Section | Content |
|---|---|---|
| 0 | **Pipeline outcomes** | Committed certification stats (rows, funnel, triggers, revenue, replay) |
| 0b | **Metadata** | Status, code root, data roots, entry points, certification script, date |
| 1 | **Verdicts** | Requirement × Proven / Documented / Not started × proof pointer |
| 2 | **Problem statement** | Business + technical problem |
| 3 | **Ask register** | Numbered asks (A1…An) |
| 4 | **Sources & sinks** | Paths, grain, write mode, idempotency keys |
| 5 | **Schemas** | Tables/columns/types; link to data dictionary |
| 6 | **Lineage & data flow** | Mermaid diagram + per-node failure behavior |
| 7 | **Per-ask implementation** | ASK → SOLUTION → CODE → EXPECTED OUTPUT for each ask |
| 8 | **Conn / disconnect matrix** | Upstream/downstream dependency × connect strategy × failure |
| 9 | **Data evolution** | Schema versioning, backward compatibility, migration notes |
| 10 | **Failure matrix** | Failure mode × behavior × recovery × owner module |
| 11 | **Fallback & resilience** | DLQ, retry, checkpoint, dedupe, graceful degradation |
| 12 | **Feedback loops** | Tests, certification gates, evidence JSON, logging |
| 13 | **Test plan** | Unit / integration / certification commands |
| 14 | **Module map** | Package tree with single responsibility per folder |
| 15 | **Scale / SLA** | Local vs production targets (honest) |
| 16 | **Document control** | Version table |

---

## 5. Code fashion & modularity

Inspired by exercise-style repos (single concern per folder) and production pipelines.

### 5.1 Package layout pattern

```
codebase/<project_name>/
  config/          # dataclass settings, env loading — no hardcoded secrets
  schemas/         # Pydantic/Avro contracts
  ingestion/       # readers, streamers, checkpoints
  state/           # Redis/Flink state adapter
  warehouse/       # bronze/silver loaders
  quality/         # DLQ, expectations, validators
  analytics/       # KPI SQL, aggregations
  evidence/        # run summary writers
  orchestration/   # pipeline coordinator only — thin wiring
codebase/scripts/  # CLI entrypoints only
tests/             # mirror behavioral contracts, not file tree
data/source/       # committed samples + attribution
data/evidence/     # committed certification outputs
data/sink/         # gitignored runtime
docs/              # design + connectivity + standards
```

### 5.2 Code rules

| Rule | Rationale |
|---|---|
| **Idempotent sinks** | At-least-once sources require deterministic keys + UPSERT/MERGE |
| **Config dataclass** | `@dataclass` or Pydantic Settings — not scattered constants |
| **Explicit logging** | `logging` with tags (`[TRIGGER]`, `[DLQ]`) — not print-only |
| **Fail open vs closed** | Document per node: invalid row → DLQ (continue) vs missing source → fail fast |
| **No notebook core logic** | Notebooks for exploration only; production path in importable modules |
| **Single orchestrator** | Business rules in orchestrator; I/O in adapters |
| **Typed contracts** | Pydantic (or equivalent) at system boundary |
| **HTTP/API errors** | Follow library patterns — e.g. requests: catch `HTTPError`, timeout, retry policy ([requests quickstart — errors](https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions)) |

### 5.3 Modularity gates

- [ ] Each folder has one reason to change
- [ ] Orchestrator < 200 lines of domain logic (delegate to adapters)
- [ ] No circular imports between ingestion ↔ warehouse
- [ ] Tests can run without network (committed sample data)
- [ ] Scripts are thin wrappers calling package APIs

### 5.4 Library maintenance & dependency catalog

Every repo MUST document **where each library comes from** and how to maintain it.

| Requirement | Standard |
|---|---|
| **Catalog file** | `docs/LIBRARIES.md` — one row per pip package |
| **Source links** | PyPI URL + upstream GitHub/homepage + official docs |
| **Pin strategy** | `requirements-runtime.txt` (minimal) + `requirements.txt` (full dev) |
| **Tested versions** | Record last green matrix (Python + package versions) |
| **Upgrade procedure** | Document: bump pin → pytest → certify → CHANGELOG |
| **Security** | Run `pip audit` on upgrade; log CVE fixes |
| **Stdlib inventory** | List stdlib modules used (no mystery imports) |
| **Future deps** | Production-only tools (Kafka, GCP) in separate table — not fake pip pins |
| **Offline proof** | Committed sample data runs without network after install |
| **Bootstrap script** | Single command runs design gates + lint + tests + certification |
| **Outcome table on README** | Stats from latest `run_summary_*.json` — see design-patterns §4.2 |

**design-commit gates:** every package in `requirements*.txt` appears in `LIBRARIES.md`; bootstrap exits 0.

Reference error-handling docs when adding HTTP clients: [requests — errors and exceptions](https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions).

---

## 6. Testing & quality pyramid

| Layer | Tool | Scope | Must prove |
|---|---|---|---|
| **Unit** | pytest | pure functions (idempotency keys, validators) | deterministic behavior |
| **Integration** | pytest + tmp_path | pipeline slices on sample/synthetic | row counts, triggers, DLQ |
| **Certification** | subprocess script | full public sample + quality gates | evidence JSON, exit 0 |
| **Lint** | ruff | style, imports, common bugs | CI green |
| **Type check** | mypy/pyright | optional but recommended | public APIs typed |

### 6.1 Required test scenarios (streaming / state pipelines)

| Scenario | Expected |
|---|---|
| Happy path ingest | N source rows → N bronze rows (or documented dedupe) |
| Invalid schema row | DLQ + stream continues |
| Idempotent replay | second run → same bronze count |
| Checkpoint resume | partial run + resume → cumulative correct |
| Trigger dedupe | campaign fires **once** per session |
| TTL / state expiry | abandonment trigger after wait window |

### 6.2 Certification script contract

Certification MUST:

1. Reset or isolate sink for clean run
2. Process full committed public sample
3. Compute KPIs post-close (no DB connection conflicts)
4. Replay for idempotency check
5. Write `data/evidence/run_summary_*.json` + index
6. Exit non-zero on any quality gate failure

---

## 7. Diagrams & graphs

Minimum diagram set:

| Diagram | Type | Location |
|---|---|---|
| Architecture | `flowchart TB` | README |
| Data lineage | `flowchart LR` | DESIGN.md |
| Session state (optional) | `stateDiagram-v2` | DESIGN.md |
| Sequence (optional) | `sequenceDiagram` | DESIGN.md for trigger path |

Diagrams must label **modules** (`ingestion/public_clickstream.py`), not vague boxes.

---

## 8. Feedback, fallback, and operations

### 8.1 Feedback loops

| Loop | Mechanism |
|---|---|
| Developer | pytest + ruff locally |
| CI | GitHub Actions on PR |
| Reviewer | evidence JSON + quality_gates booleans |
| Runtime | structured logs + DLQ artifacts |

### 8.2 Fallback patterns

| Pattern | When |
|---|---|
| **DLQ quarantine** | poison messages / schema violations |
| **Checkpoint resume** | process crash mid-batch |
| **UPSERT idempotency** | duplicate delivery |
| **Trigger dedupe keys** | prevent duplicate campaigns |
| **Synthetic path** | prove behavior public data cannot show |

Document explicitly what is **not** implemented (e.g. no Kafka yet) vs emulated locally.

---

## 9. Changelog discipline

Use `CHANGELOG.md` with sections: `Added`, `Changed`, `Fixed`, `Deprecated`, `Removed`, `Security`.

- Every merge-worthy PR updates CHANGELOG under `## [Unreleased]` or a dated version
- Link changelog entries to asks (A1, A2) when behavior changes
- Certification evidence refreshed when gates or row counts change

---

## 10. Git & branching (data engineers)

Pragmatic workflow ([git for data engineers](https://medium.com/@mahajansd/git-for-data-engineers-how-much-you-really-need-to-know-and-how-i-actually-use-it-97c6d57125d5)):

| Practice | Standard |
|---|---|
| Branch | `feature/<short-name>` or `docs/<short-name>` off `main` |
| Commits | Imperative subject; body explains **why** |
| PR | Links DESIGN verdict + test output |
| Never | Force-push `main`, commit secrets, commit `data/sink/` runtime |
| Always | Regenerate evidence when pipeline logic changes |

---

## 11. Tooling categories checklist (from awesome lists)

When scoping a project, name which categories you touch:

| Category | Example tools | This repo (cart abandonment) |
|---|---|---|
| Ingestion | Kafka, Pub/Sub, HTTP, CSV | CSV streamer (Pub/Sub emulator) |
| Stream processing | Flink, Dataflow, custom | Python orchestrator |
| State | Redis, Memorystore | LocalRedisStore TTL |
| Warehouse | BigQuery, DuckDB, Snowflake | DuckDB bronze |
| Quality | Great Expectations, DLQ | Pydantic + DLQ JSON |
| Orchestration | Airflow, Dagster | CLI scripts |
| Evidence | dbt tests, custom JSON | certify_public_run.py |

---

## 12. Portfolio verdict rubric

| Grade | Criteria |
|---|---|
| **A — Portfolio ready** | All §2 artifacts, certification green, CONNECTIVITY complete, CHANGELOG current |
| **B — Engineering sound** | Code + tests pass; docs missing connectivity or changelog |
| **C — Demo only** | Runs locally; no evidence, vague README, no failure matrix |
| **F — Misleading** | Claims production scale without proof; random IDs; no idempotency |

---

## 13. References

- [danielbeach/data-engineering-practice](https://github.com/danielbeach/data-engineering-practice) — exercise structure, Docker, progressive difficulty
- [igorbarinov/awesome-data-engineering](http://github.com/igorbarinov/awesome-data-engineering) — tooling landscape
- [pracdata/awesome-open-source-data-engineering](https://github.com/pracdata/awesome-open-source-data-engineering) — OSS analytics stack
- [KDnuggets — Learn DE from GitHub repos](https://www.kdnuggets.com/2023/02/learn-data-engineering-github-repositories.html) — roadmap + project learning
- [requests — errors and exceptions](https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions) — HTTP client error handling pattern
