# Data Dictionary — Fleet Checkout Streaming Pipeline

## Source: rental CDC (`data/source/samples/fleet_rental_cdc.jsonl`)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `op` | string | no | Debezium CDC op: `c`, `r`, `u`, `d` |
| `event_time` | string (UTC timestamp) | no | When the rental state change occurred |
| `event_type` | string | no | `reservation_started`, `checkout_complete`, `vehicle_returned`, etc. |
| `rental_id` | string | no | Rental booking identifier |
| `vehicle_id` | string | no | Fleet vehicle identifier |
| `customer_id` | string | no | Customer identifier (synthetic) |
| `location_id` | string | no | Pickup/dropoff location code |
| `daily_rate` | float | no | Daily rental rate (must be >= 0) |
| `vehicle_class` | string | no | `economy`, `compact`, `suv`, `luxury` |
| `status` | string | no | Vehicle/rental status at event time |

**Grain:** one row = one CDC change event on a rental or vehicle record.

**Attribution:** Portfolio-generated synthetic sample; no PII.

---

## Sink: bronze table (`bronze_rental_cdc` in DuckDB)

| Column | Type | Key | Description |
|---|---|---|---|
| `cdc_id` | VARCHAR | PK | Deterministic hash idempotency key |
| `op` | VARCHAR | | CDC operation |
| `event_time` | TIMESTAMP | | Event timestamp |
| `event_type` | VARCHAR | | Rental lifecycle event |
| `rental_id` | VARCHAR | | Rental id |
| `vehicle_id` | VARCHAR | | Vehicle id |
| `customer_id` | VARCHAR | | Customer id |
| `location_id` | VARCHAR | | Location code |
| `daily_rate` | DOUBLE | | Daily rate |
| `vehicle_class` | VARCHAR | | Vehicle class |
| `status` | VARCHAR | | Status snapshot |
| `_ingested_at` | TIMESTAMP | | Pipeline ingest time |

---

## Sink: silver table (`silver_rental_sessions`)

| Column | Type | Key | Description |
|---|---|---|---|
| `rental_id` | VARCHAR | PK | Rental session grain |
| `vehicle_id` | VARCHAR | | Latest vehicle |
| `reservation_started_at` | TIMESTAMP | | First reservation event |
| `checkout_at` | TIMESTAMP | | Checkout complete time |
| `returned_at` | TIMESTAMP | | Vehicle return time |
| `final_status` | VARCHAR | | Latest status |
| `max_daily_rate` | DOUBLE | | Peak rate in session |

---

## Sink: gold table (`gold_fleet_kpis`)

| Column | Type | Key | Description |
|---|---|---|---|
| `metric_name` | VARCHAR | PK | `fleet_utilization_pct`, `booking_conversion_pct` |
| `metric_value` | DOUBLE | | Computed KPI value |
| `computed_at` | TIMESTAMP | | KPI refresh time |

---

## Evidence artifact (`data/evidence/run_summary_*.json`)

| Field | Description |
|---|---|
| `source` | Dataset path + attribution |
| `pipeline` | events processed, checkpoint offset, inventory snapshot |
| `warehouse_kpis` | bronze counts, event mix |
| `gold_kpis` | utilization + conversion |
| `quality_gates` | pass/fail booleans for certification |
