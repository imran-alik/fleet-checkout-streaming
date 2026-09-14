-- Fleet rental streaming KPIs (bronze / silver / gold)
-- Run after: py codebase/scripts/certify_public_run.py
-- Connect: duckdb data/sink/warehouse.db  (or attach read-only in DuckDB CLI)

-- 1. CDC event volume by type
SELECT
    event_type,
    COUNT(*) AS event_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_events
FROM bronze_rental_cdc
GROUP BY 1
ORDER BY event_count DESC;

-- 2. Rental funnel: reservations vs checkouts
WITH rental_flags AS (
    SELECT
        rental_id,
        MAX(CASE WHEN event_type = 'reservation_started' THEN 1 ELSE 0 END) AS has_reservation,
        MAX(CASE WHEN event_type = 'checkout_complete' THEN 1 ELSE 0 END) AS has_checkout
    FROM bronze_rental_cdc
    GROUP BY 1
)
SELECT
    COUNT(*) AS total_rentals,
    SUM(has_reservation) AS reservations_started,
    SUM(has_checkout) AS checkouts_completed,
    ROUND(100.0 * SUM(has_checkout) / NULLIF(SUM(has_reservation), 0), 2) AS booking_conversion_pct
FROM rental_flags;

-- 3. Gold layer KPIs (certified run)
SELECT metric_name, metric_value, computed_at
FROM gold_fleet_kpis
ORDER BY metric_name;

-- Expected on certified sample (200 events):
-- bronze_rows = 200, booking_conversion_pct > 0, fleet_utilization_pct > 0, dlq = 0
