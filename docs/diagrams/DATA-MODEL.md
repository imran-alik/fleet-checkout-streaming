# Data Model & ERD — Fleet Checkout Streaming

CDC rental events → Kafka bus → DuckDB medallion + Redis live inventory.

Reference: [uber-data-engineering-mage-project](https://github.com/darshilparmar/uber-data-engineering-mage-project) data-model diagram pattern.

Cross-reference: [data-dictionary.md](../../data-dictionary.md) · [DESIGN.md](../DESIGN.md)

---

## 1. Source CDC event model

Each JSONL line is a Debezium-style change event on a rental/vehicle record.

```mermaid
erDiagram
    RENTAL_CDC_EVENT {
        string rental_id "business key"
        string vehicle_id
        string customer_id
        string location_id
        string event_type
        string op "c|r|u|d"
        timestamp event_time
        float daily_rate
        string vehicle_class
        string status
    }
```

**Grain:** one row = one CDC state change (not one rental session).

---

## 2. Medallion warehouse ERD

```mermaid
erDiagram
    BRONZE_RENTAL_CDC ||--|| SILVER_RENTAL_SESSIONS : "sessionized by rental_id"
    SILVER_RENTAL_SESSIONS ||--o{ GOLD_FLEET_KPIS : "aggregated metrics"

    BRONZE_RENTAL_CDC {
        string cdc_id PK
        string rental_id
        string vehicle_id
        string event_type
        timestamp event_time
    }
    SILVER_RENTAL_SESSIONS {
        string rental_id PK
        timestamp reservation_started_at
        timestamp checkout_at
        timestamp returned_at
        string final_status
    }
    GOLD_FLEET_KPIS {
        string metric_name PK
        float metric_value
        timestamp computed_at
    }
```

---

## 3. Streaming + inventory topology

Parallel paths: warehouse medallion and Redis live state.

```mermaid
flowchart LR
    JSONL[fleet_rental_cdc.jsonl] --> KAFKA[Kafka topic emulator]
    KAFKA --> BRZ[(bronze_rental_cdc)]
    KAFKA --> REDIS[(Redis inventory hash)]
    BRZ --> SLV[(silver_rental_sessions)]
    SLV --> GLD[(gold_fleet_kpis)]
    REDIS --> KPI[Utilization KPI]
    GLD --> EV[Evidence JSON]
```

---

## 4. Event lifecycle (rental session)

```mermaid
stateDiagram-v2
    [*] --> reservation_started
    reservation_started --> checkout_complete
    checkout_complete --> vehicle_returned
    vehicle_returned --> [*]
    reservation_started --> cancelled : optional
```

Session silver table collapses multiple CDC events into one row per `rental_id`.
