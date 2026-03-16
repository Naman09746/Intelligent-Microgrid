"""
edge/database.py
================
Private SQLite database manager — one instance per home node.

Design principles
-----------------
- Each node writes to its own isolated file: data/edge/node_{id}.db
- Granular telemetry never leaves this file; only NodeSummary is exposed.
- All public methods are synchronous and thread-safe (SQLite WAL mode).
- Connections are kept open for the lifetime of the object and closed via close().
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional

from edge.models import (
    CREATE_TELEMETRY_INDEX,
    CREATE_TELEMETRY_TABLE,
    NodeSummary,
    TelemetryReading,
)

logger = logging.getLogger("Edge.Database")

# Surplus/deficit threshold in kW (matches market_summarizer heuristic: 200 W)
_INTENT_THRESHOLD_KW = 0.2


class EdgeDatabase:
    """
    Manages a private SQLite database for a single home edge node.

    Usage
    -----
    db = EdgeDatabase("delhi_01")
    db.initialize()
    db.insert_reading(reading)
    summary = db.get_summary(hours=1)
    db.cleanup()
    db.close()
    """

    def __init__(self, node_id: str, db_dir: str = os.path.join("data", "edge")):
        self.node_id = node_id
        self.db_dir  = db_dir
        os.makedirs(db_dir, exist_ok=True)

        db_path = os.path.join(db_dir, f"node_{node_id}.db")
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
        self._conn.row_factory = sqlite3.Row

        # WAL mode: allows concurrent reads while a write is in progress
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")   # Good balance of safety/speed

        logger.info(f"[{node_id}] Database opened at {db_path}")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def initialize(self) -> None:
        """Create tables and indexes if they don't already exist."""
        with self._conn:
            self._conn.execute(CREATE_TELEMETRY_TABLE)
            self._conn.execute(CREATE_TELEMETRY_INDEX)
        logger.info(f"[{self.node_id}] Schema initialised.")

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------
    def insert_reading(self, reading: TelemetryReading) -> None:
        """Persist a single telemetry snapshot."""
        self._upsert([reading])

    def insert_batch(self, readings: List[TelemetryReading]) -> None:
        """Persist multiple readings efficiently in one transaction."""
        if not readings:
            return
        self._upsert(readings)
        logger.debug(f"[{self.node_id}] Batch inserted {len(readings)} readings.")

    def _upsert(self, readings: List[TelemetryReading]) -> None:
        sql = """
            INSERT INTO telemetry
                (node_id, timestamp, voltage_v, current_a,
                 power_solar_kw, power_load_kw, soc_pct,
                 battery_power_kw, grid_import_kw, grid_export_kw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (r.node_id, r.timestamp, r.voltage_v, r.current_a,
             r.power_solar_kw, r.power_load_kw, r.soc_pct,
             r.battery_power_kw, r.grid_import_kw, r.grid_export_kw)
            for r in readings
        ]
        with self._conn:
            self._conn.executemany(sql, rows)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def get_latest(self, n: int = 1) -> List[TelemetryReading]:
        """Return the most recent N readings, newest first."""
        cur = self._conn.execute(
            "SELECT * FROM telemetry WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?",
            (self.node_id, n),
        )
        return [TelemetryReading.from_sqlite_row(row) for row in cur.fetchall()]

    def get_range(self, start: datetime, end: datetime) -> List[TelemetryReading]:
        """Return all readings whose timestamp falls within [start, end]."""
        cur = self._conn.execute(
            """SELECT * FROM telemetry
               WHERE node_id = ?
                 AND timestamp >= ?
                 AND timestamp <= ?
               ORDER BY timestamp ASC""",
            (self.node_id, start.isoformat(), end.isoformat()),
        )
        return [TelemetryReading.from_sqlite_row(row) for row in cur.fetchall()]

    def get_summary(self, hours: int = 1) -> Optional[NodeSummary]:
        """
        Return an aggregated NodeSummary covering the last `hours` of data.
        Falls back to ALL available rows if the time-window is empty
        (handles simulated timestamps that may not be in UTC).
        Returns None only if the database is completely empty.
        """
        since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        cur = self._conn.execute(
            """SELECT
                   COUNT(*)            AS cnt,
                   AVG(power_load_kw)  AS avg_load,
                   AVG(power_solar_kw) AS avg_solar,
                   MAX(timestamp)      AS last_ts
               FROM telemetry
               WHERE node_id = ? AND timestamp >= ?""",
            (self.node_id, since),
        )
        row = cur.fetchone()

        # Fallback: if window query is empty (e.g. simulated past/future timestamps),
        # aggregate over ALL rows for this node
        if not row or row["cnt"] == 0:
            cur = self._conn.execute(
                """SELECT COUNT(*) AS cnt,
                          AVG(power_load_kw)  AS avg_load,
                          AVG(power_solar_kw) AS avg_solar,
                          MAX(timestamp)      AS last_ts
                   FROM telemetry WHERE node_id = ?""",
                (self.node_id,),
            )
            row = cur.fetchone()

        if not row or row["cnt"] == 0:
            return None

        # Latest SoC — fetch from most recent row
        latest = self.get_latest(1)
        current_soc = latest[0].soc_pct if latest else 0.0

        avg_load  = round(row["avg_load"]  or 0.0, 4)
        avg_solar = round(row["avg_solar"] or 0.0, 4)
        net       = round(avg_solar - avg_load, 4)

        if net > _INTENT_THRESHOLD_KW:
            intent = "SURPLUS"
        elif net < -_INTENT_THRESHOLD_KW:
            intent = "DEFICIT"
        else:
            intent = "BALANCED"

        return NodeSummary(
            node_id         = self.node_id,
            as_of           = row["last_ts"],
            avg_load_kw     = avg_load,
            avg_solar_kw    = avg_solar,
            current_soc_pct = round(current_soc, 1),
            net_energy_kw   = net,
            intent          = intent,
            sample_count    = row["cnt"],
        )

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------
    def cleanup(self, retention_hours: Optional[int] = None) -> int:
        """
        Delete readings older than `retention_hours`.
        Returns the number of rows deleted.
        """
        from edge.config import DATA_RETENTION_HOURS
        cutoff_hours = retention_hours if retention_hours is not None else DATA_RETENTION_HOURS
        cutoff = (datetime.utcnow() - timedelta(hours=cutoff_hours)).isoformat()

        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM telemetry WHERE node_id = ? AND timestamp < ?",
                (self.node_id, cutoff),
            )
        deleted = cur.rowcount
        if deleted:
            logger.info(f"[{self.node_id}] Cleaned up {deleted} rows older than {cutoff_hours}h.")
        return deleted

    def row_count(self) -> int:
        """Return total number of rows for this node (useful for health checks)."""
        cur = self._conn.execute(
            "SELECT COUNT(*) FROM telemetry WHERE node_id = ?", (self.node_id,)
        )
        return cur.fetchone()[0]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close the database connection cleanly."""
        self._conn.close()
        logger.info(f"[{self.node_id}] Database connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
