from __future__ import annotations

import logging

import duckdb

from fleet_streaming.schemas.events import RentalCdcEvent

log = logging.getLogger(__name__)


class DuckDBWarehouse:
    """Bronze / silver / gold medallion loader (local DuckDB; production: BigQuery)."""

    def __init__(
        self,
        db_path: str,
        bronze_table: str,
        silver_table: str,
        gold_table: str,
        *,
        batch_size: int = 50,
    ) -> None:
        self.db_path = db_path
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.batch_size = batch_size
        self.conn = duckdb.connect(db_path)
        self._buffer: list[RentalCdcEvent] = []
        self._configure()
        self._init_schema()

    def _configure(self) -> None:
        self.conn.execute("SET memory_limit='2GB'")
        self.conn.execute("SET threads=4")
        self.conn.execute("SET preserve_insertion_order=false")

    def _init_schema(self) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.bronze_table} (
                cdc_id VARCHAR PRIMARY KEY,
                op VARCHAR,
                event_time TIMESTAMP,
                event_type VARCHAR,
                rental_id VARCHAR,
                vehicle_id VARCHAR,
                customer_id VARCHAR,
                location_id VARCHAR,
                daily_rate DOUBLE,
                vehicle_class VARCHAR,
                status VARCHAR,
                _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.silver_table} (
                rental_id VARCHAR PRIMARY KEY,
                vehicle_id VARCHAR,
                customer_id VARCHAR,
                location_id VARCHAR,
                vehicle_class VARCHAR,
                reservation_started_at TIMESTAMP,
                checkout_at TIMESTAMP,
                returned_at TIMESTAMP,
                final_status VARCHAR,
                max_daily_rate DOUBLE,
                _refreshed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.gold_table} (
                metric_name VARCHAR PRIMARY KEY,
                metric_value DOUBLE,
                computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def enqueue(self, event: RentalCdcEvent) -> None:
        self._buffer.append(event)
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        rows = [
            (
                e.cdc_id,
                e.op,
                e.event_time,
                e.event_type,
                e.rental_id,
                e.vehicle_id,
                e.customer_id,
                e.location_id,
                e.daily_rate,
                e.vehicle_class,
                e.status,
            )
            for e in self._buffer
        ]
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.executemany(
                f"""
                INSERT OR REPLACE INTO {self.bronze_table} (
                    cdc_id, op, event_time, event_type, rental_id, vehicle_id,
                    customer_id, location_id, daily_rate, vehicle_class, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        finally:
            self._buffer.clear()

    def build_silver(self) -> int:
        self.conn.execute(
            f"""
            INSERT OR REPLACE INTO {self.silver_table}
            SELECT
                rental_id,
                MAX(vehicle_id) AS vehicle_id,
                MAX(customer_id) AS customer_id,
                MAX(location_id) AS location_id,
                MAX(vehicle_class) AS vehicle_class,
                MIN(CASE WHEN event_type = 'reservation_started' THEN event_time END) AS reservation_started_at,
                MIN(CASE WHEN event_type = 'checkout_complete' THEN event_time END) AS checkout_at,
                MIN(CASE WHEN event_type = 'vehicle_returned' THEN event_time END) AS returned_at,
                MAX_BY(status, event_time) AS final_status,
                MAX(daily_rate) AS max_daily_rate,
                CURRENT_TIMESTAMP AS _refreshed_at
            FROM {self.bronze_table}
            GROUP BY rental_id
            """
        )
        return int(self.conn.execute(f"SELECT COUNT(*) FROM {self.silver_table}").fetchone()[0])

    def build_gold(self) -> dict[str, float]:
        self.conn.execute(f"DELETE FROM {self.gold_table}")
        metrics = self.conn.execute(
            f"""
            WITH fleet AS (
                SELECT COUNT(DISTINCT vehicle_id) AS total_vehicles
                FROM {self.bronze_table}
            ),
            rented AS (
                SELECT COUNT(DISTINCT vehicle_id) AS rented_vehicles
                FROM {self.bronze_table}
                WHERE event_type IN ('checkout_complete')
                  AND status = 'rented'
            ),
            reservations AS (
                SELECT COUNT(DISTINCT rental_id) AS started
                FROM {self.bronze_table}
                WHERE event_type = 'reservation_started'
            ),
            conversions AS (
                SELECT COUNT(DISTINCT rental_id) AS completed
                FROM {self.bronze_table}
                WHERE event_type = 'checkout_complete'
            )
            SELECT
                ROUND(100.0 * r.rented_vehicles / NULLIF(f.total_vehicles, 0), 2) AS fleet_utilization_pct,
                ROUND(100.0 * c.completed / NULLIF(rs.started, 0), 2) AS booking_conversion_pct
            FROM fleet f, rented r, reservations rs, conversions c
            """
        ).fetchone()
        utilization = float(metrics[0] or 0.0)
        conversion = float(metrics[1] or 0.0)
        rows = [
            ("fleet_utilization_pct", utilization),
            ("booking_conversion_pct", conversion),
        ]
        self.conn.executemany(
            f"INSERT INTO {self.gold_table} (metric_name, metric_value) VALUES (?, ?)",
            rows,
        )
        return {"fleet_utilization_pct": utilization, "booking_conversion_pct": conversion}

    def bronze_count(self) -> int:
        return int(self.conn.execute(f"SELECT COUNT(*) FROM {self.bronze_table}").fetchone()[0])

    def count(self) -> int:
        return self.bronze_count()

    def close(self) -> None:
        self.flush()
        try:
            self.conn.execute("CHECKPOINT")
        finally:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]
