# Libraries — Fleet Checkout Streaming

Dependency catalog for `codebase/requirements*.txt`. No secrets in repo.

---

## Runtime

| Package | Version | Purpose |
|---|---|---|
| **duckdb** | >=1.0.0 | Local medallion warehouse (bronze/silver/gold) |
| **pydantic** | >=2.0.0 | CDC event schema validation |

---

## Development / test

| Package | Version | Purpose |
|---|---|---|
| **pytest** | >=8.0.0 | Unit and integration tests |
| **ruff** | >=0.8.0 | Lint and import sorting |

---

## Production equivalents (not installed locally)

| Concern | GCP / OSS |
|---|---|
| Streaming bus | Managed Kafka, Pub/Sub |
| CDC source | Debezium, Flink CDC, Datastream |
| State store | Memorystore Redis |
| Warehouse | BigQuery |
| Orchestration | Cloud Composer, Flink on GKE |

---

## Maintenance

- Pin major versions in requirements files
- Run `bootstrap.ps1` before merge to verify lint + tests + certification
- Document any new dependency here before adding to requirements
