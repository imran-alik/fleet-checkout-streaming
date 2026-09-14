# GCP & GCS Service Mapping

> Local emulator → production GCP for fleet rental CDC streaming.

---

## Service map

| Local (this repo) | GCP service | Role |
|---|---|---|
| `cdc_jsonl.py` + `kafka_queue.py` | **Managed Kafka** or **Cloud Pub/Sub** | CDC event bus |
| Rental DB (production) | **Cloud SQL / AlloyDB** + **Debezium / Datastream** | Source CDC |
| `orchestration/pipeline.py` | **Cloud Run**, **Dataflow**, or **Flink on GKE** | Stateful stream processing |
| `state/redis_store.py` | **Memorystore (Redis)** | Live fleet inventory + hold TTL |
| `warehouse/duckdb_loader.py` | **BigQuery** medallion | Bronze MERGE + silver/gold views |
| `quality/dlq.py` | **GCS** + Pub/Sub DLQ topic | Poison-message quarantine |
| `evidence/writer.py` | **GCS** | Run summary / certification JSON |
| `scripts/*.py` | **Cloud Composer** (Airflow) | Schedule + dependency orchestration |

---

## GCS layout (recommended)

```
gs://{project}-fleet-{env}/
├── landing/cdc/                      # optional batch CDC backfill (JSONL/Avro)
├── dlq/quarantine/                   # invalid events (JSON)
├── evidence/run_summary/             # certification / audit JSON
└── checkpoints/ingest/               # offset metadata
```

BigQuery datasets:

- `{project}.fleet_bronze` · `bronze_rental_cdc`
- `{project}.fleet_silver` · `silver_rental_sessions`
- `{project}.fleet_gold` · `gold_fleet_kpis`

---

## Pub/Sub / Kafka topics (recommended)

| Topic | Publisher | Subscriber |
|---|---|---|
| `rental.cdc.v1` | Debezium / Flink CDC | Stream processor |
| `fleet-inventory-delta` | Pipeline (on state change) | Ops dashboard |
| `dlq-rental-cdc` | Pipeline (on validation fail) | Ops alert + GCS sink |

---

## IAM (minimum)

| Principal | Access |
|---|---|
| Composer SA | Pub/Sub subscribe, BQ dataEditor, GCS objectAdmin |
| Stream processor SA | Memorystore, Pub/Sub, BQ, GCS |
| CI | None on prod by default — deploy via WIF + Terraform |

Secrets: **Secret Manager** for DB credentials, Redis URL — not in repo ([`env/.env.example`](../../env/.env.example)).

---

## Migration checklist

1. Replace `InProcessKafkaQueue` with Pub/Sub client or Kafka consumer group
2. Replace `LocalRedisStore` with Memorystore Redis client
3. Replace DuckDB UPSERT with BigQuery `MERGE` on `cdc_id`
4. Materialize silver/gold as scheduled BQ queries or dbt models
5. Wire Composer DAG: ingest → validate → medallion refresh → evidence export to GCS
