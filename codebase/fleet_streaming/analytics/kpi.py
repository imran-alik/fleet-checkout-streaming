from __future__ import annotations

from typing import Any

import duckdb


def compute_bronze_kpis(
    db_path: str | None = None,
    bronze_table: str = "bronze_rental_cdc",
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    own_conn = conn is None
    if own_conn:
        if db_path is None:
            raise ValueError("db_path required when conn is not provided")
        conn = duckdb.connect(db_path, read_only=True)
    try:
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {bronze_table}").fetchone()[0]
        distinct_rentals = conn.execute(
            f"SELECT COUNT(DISTINCT rental_id) FROM {bronze_table}"
        ).fetchone()[0]
        distinct_vehicles = conn.execute(
            f"SELECT COUNT(DISTINCT vehicle_id) FROM {bronze_table}"
        ).fetchone()[0]
        event_mix = conn.execute(
            f"""
            SELECT event_type, COUNT(*) AS event_count
            FROM {bronze_table}
            GROUP BY 1
            ORDER BY event_count DESC
            """
        ).fetchall()
        reservations_started = conn.execute(
            f"""
            SELECT COUNT(DISTINCT rental_id)
            FROM {bronze_table}
            WHERE event_type = 'reservation_started'
            """
        ).fetchone()[0]
        checkouts = conn.execute(
            f"""
            SELECT COUNT(DISTINCT rental_id)
            FROM {bronze_table}
            WHERE event_type = 'checkout_complete'
            """
        ).fetchone()[0]
        return {
            "bronze_rows": int(total_rows),
            "distinct_rentals": int(distinct_rentals),
            "distinct_vehicles": int(distinct_vehicles),
            "event_type_mix": {row[0]: int(row[1]) for row in event_mix},
            "reservations_started": int(reservations_started),
            "checkouts_completed": int(checkouts),
        }
    finally:
        if own_conn:
            conn.close()


def compute_gold_kpis(
    db_path: str | None = None,
    gold_table: str = "gold_fleet_kpis",
    *,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, float]:
    own_conn = conn is None
    if own_conn:
        if db_path is None:
            raise ValueError("db_path required when conn is not provided")
        conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(f"SELECT metric_name, metric_value FROM {gold_table}").fetchall()
        return {str(name): float(value) for name, value in rows}
    finally:
        if own_conn:
            conn.close()


def summarize_triggers(triggers: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for trigger in triggers:
        campaign = trigger["campaign_type"]
        by_type[campaign] = by_type.get(campaign, 0) + 1
    return {
        "total_triggers": len(triggers),
        "by_campaign_type": by_type,
        "sample_triggers": triggers[:5],
    }
