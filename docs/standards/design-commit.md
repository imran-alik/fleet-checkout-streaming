# Design Commit — Pre-Merge Gate Checklist

> Run this checklist **before every merge-worthy commit or PR** on a data engineering case study or pipeline repo.  
> Companion to [`data-engineering-design-standards.md`](./data-engineering-design-standards.md) and [`design-patterns.md`](./design-patterns.md).

**How to use:** Copy this checklist into your PR description or run `codebase/scripts/check_design_commit.py` (if present). Every **Gate** must pass or be explicitly waived with reason in CHANGELOG.

---

## A. Branch & git hygiene

| # | Gate | Pass? | Notes |
|---|---|---|---|
| A1 | Branch created from latest `main` (not stale fork) | ☐ | `feature/*`, `docs/*`, `fix/*` |
| A2 | No secrets, `.env`, credentials, or tokens in diff | ☐ | Use `env/.env.example` only |
| A3 | `data/sink/`, `*.db`, `.pytest_cache/` not staged | ☐ | Runtime outputs gitignored |
| A4 | Commit message: imperative subject + **why** in body | ☐ | Link ask ID if behavior change |
| A5 | CHANGELOG.md updated under `[Unreleased]` or version | ☐ | Keep a Changelog format |
| A6 | If behavior changed: new branch commit, not silent amend of pushed work | ☐ | See git safety rules |

---

## B. Problem & narrative (does it make sense?)

| # | Gate | Pass? | Notes |
|---|---|---|---|
| B1 | README **Pipeline Outcomes** table with committed certification stats | ☐ | Matches latest `run_summary_*.json` |
| B2 | Tech stack + data model sections present | ☐ | design-patterns §1, §3 |
| B3 | Pipeline scripts linked (extract/transform/load/analytics) | ☐ | Uber-style module links |
| B4 | A reviewer finds verify steps in ≤2 clicks | ☐ | Getting started + certify command |
| B5 | Claims labeled: **Proven** vs **Documented** vs **Planned** | ☐ | Match DESIGN verdicts |
| B6 | No tutorial/BTS language in user-facing docs | ☐ | No “plug and play”, “simple”, “layman only” |
| B7 | Public data attribution present | ☐ | URL + license in README/DESIGN |
| B8 | Production roadmap honest (not fake completeness) | ☐ | GCP/Kafka/GE as documented follow-ups |

---

## C. Design modularity & usability

| # | Gate | Pass? | Notes |
|---|---|---|---|
| C1 | Package folders match single-responsibility layout | ☐ | config/ ingestion/ state/ warehouse/ quality/ orchestration/ |
| C2 | Orchestrator wires adapters; no 500-line god script | ☐ | `orchestration/pipeline.py` thin domain |
| C3 | Config centralized (`settings.py` / env) | ☐ | No hardcoded paths in tests only via `for_tests()` |
| C4 | CLI scripts are thin entrypoints | ☐ | `run_pipeline.py`, `certify_public_run.py` |
| C5 | New engineer can run certify + pytest without asking author | ☐ | HOW-TO-EXECUTE.md works copy-paste |
| C6 | Docker/compose documented if used | ☐ | N/A if local-only |

---

## D. Code ↔ doc ↔ file connectivity

| # | Gate | Pass? | Notes |
|---|---|---|---|
| D1 | `docs/CONNECTIVITY.md` maps every ask → module → test → doc section | ☐ | Updated if files moved |
| D2 | Every module in DESIGN §14 exists on disk | ☐ | No ghost paths |
| D3 | `data-dictionary.md` columns match bronze DDL | ☐ | Types align |
| D4 | SOLUTION.md ask IDs match DESIGN ask register | ☐ | A1…An consistent |
| D5 | README links resolve (DESIGN, SOLUTION, evidence index) | ☐ | No broken relative links |
| D6 | Evidence JSON field names match data-dictionary evidence section | ☐ | |

**Connectivity spot-check command:**

```powershell
py -3.12 codebase\scripts\check_design_commit.py
```

---

## E. Design document completeness

| # | Gate | Pass? | Notes |
|---|---|---|---|
| E1 | DESIGN.md has verdicts table with proof pointers | ☐ | |
| E2 | Source/sink schemas with idempotency keys | ☐ | |
| E3 | Lineage diagram + per-node failure behavior | ☐ | |
| E4 | Conn/disconnect matrix present | ☐ | |
| E5 | Data evolution section present | ☐ | Even if “v1 only” |
| E6 | Failure matrix + fallback section present | ☐ | DLQ, checkpoint, UPSERT |
| E7 | Each ask has ASK → SOLUTION → CODE → OUTPUT | ☐ | In DESIGN or SOLUTION |
| E8 | Document control version row updated | ☐ | |

---

## F. Tests, lint, certification

| # | Gate | Pass? | Notes |
|---|---|---|---|
| F1 | `py -3.12 -m pytest -q` → all green | ☐ | |
| F2 | `py -3.12 codebase\scripts\certify_public_run.py` → exit 0 | ☐ | If public data touched |
| F3 | Ruff lint passes (if configured) | ☐ | `ruff check codebase tests` |
| F4 | Idempotent replay test still passes | ☐ | Critical for streaming |
| F5 | Trigger dedupe test still passes | ☐ | One campaign per session |
| F6 | New behavior has new or updated test | ☐ | No untested asks |

---

## G. Evidence & quality gates

| # | Gate | Pass? | Notes |
|---|---|---|---|
| G1 | `data/evidence/run_summary_index.json` updated | ☐ | When certification re-run |
| G2 | Quality gates all `true` in latest evidence | ☐ | See certify script output |
| G3 | Row counts in evidence match README gate table | ☐ | 400/400 etc. |
| G4 | Old failed evidence artifacts removed from commit | ☐ | Don't commit red runs |

---

## H. Libraries & dependency maintenance

| # | Gate | Pass? | Notes |
|---|---|---|---|
| H1 | `docs/LIBRARIES.md` exists with PyPI + docs link per pip package | ☐ | |
| H2 | `requirements-runtime.txt` contains only runtime deps | ☐ | duckdb, pydantic, etc. |
| H3 | Every line in `requirements*.txt` documented in LIBRARIES.md | ☐ | No orphan deps |
| H4 | Last verified version matrix updated if pins changed | ☐ | §8 in LIBRARIES.md |
| H5 | `pip audit` run after dependency upgrade (or N/A noted) | ☐ | |
| H6 | Bootstrap script exits 0 on clean machine | ☐ | `bootstrap.ps1` / `bootstrap.sh` |
| H7 | Public sample run works offline post-`pip install` | ☐ | No network for certify |

---

## I. Reviewer experience (final pass)

| # | Gate | Pass? | Notes |
|---|---|---|---|
| I1 | Clone → install → certify works on clean machine | ☐ | Python 3.12+ only dep |
| I2 | Architecture diagram renders in GitHub Markdown | ☐ | Mermaid valid |
| I3 | No placeholder TODO blocks in user-facing docs | ☐ | |
| I4 | SCALE-OUT / cloud notes clearly separated from local proof | ☐ | archive/ folder OK |

---

## Merge verdict

| Result | Action |
|---|---|
| **All gates pass** | Merge PR; tag release if milestone |
| **≤2 waivers** | Document waiver + follow-up issue in CHANGELOG |
| **Any critical fail** (F1, F2, B3, D4, E7) | Do not merge |

Critical fails: tests red, certification red, misleading proven claims, ask/doc drift.

---

## Quick pre-commit command block

```powershell
cd <repo-root>
.\codebase\scripts\bootstrap.ps1
# or manually:
$env:PYTHONPATH="codebase"
ruff check codebase tests
py -3.12 -m pytest -q
py -3.12 codebase\scripts\certify_public_run.py
py -3.12 codebase\scripts\check_design_commit.py
```

---

## Changelog entry template (on merge)

```markdown
## [Unreleased]
### Added
- ...
### Changed
- ...
### Fixed
- ...
```

---

## Reference repos (what “good” looks like)

| Repo | Learn |
|---|---|
| [vishal-bulbule](https://github.com/vishal-bulbule) | Portfolio front page, featured project framing, production-grade tone |
| [uber-data-engineering-mage-project](https://github.com/darshilparmar/uber-data-engineering-mage-project) | Architecture, ETL script links, `analytics_query.sql` |
| [Retail_Analysis_Redshift](https://github.com/ansamAY/Retail_Analysis_Redshift) | Tech stack, dim/fact/mart layers, numbered getting started |
| [danielbeach/data-engineering-practice](https://github.com/danielbeach/data-engineering-practice) | Per-exercise README, Docker, progressive tests |
| [vishal-bulbule/etl-pipeline-datafusion-airflow](https://github.com/vishal-bulbule/etl-pipeline-datafusion-airflow) | End-to-end GCP ETL with Composer |
