"""
edge/models.py
==============
Data models for the Edge Data Layer.

- TelemetryReading : one sensor snapshot (published via MQTT, persisted to SQLite)
- NodeSummary      : aggregated stats exposed to the Strategic LLM Agent / Orchestrator
- SQL schema constants used by EdgeDatabase
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# SQLite schema — single normalised table per node (privacy-by-design)
# ---------------------------------------------------------------------------

CREATE_TELEMETRY_TABLE = """
CREATE TABLE IF NOT EXISTS telemetry (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    node_id           TEXT     NOT NULL,
    timestamp         TEXT     NOT NULL,
    voltage_v         REAL     NOT NULL DEFAULT 0.0,
    current_a         REAL     NOT NULL DEFAULT 0.0,
    power_solar_kw    REAL     NOT NULL DEFAULT 0.0,
    power_load_kw     REAL     NOT NULL DEFAULT 0.0,
    soc_pct           REAL     NOT NULL DEFAULT 0.0,
    battery_power_kw  REAL     NOT NULL DEFAULT 0.0,
    grid_import_kw    REAL     NOT NULL DEFAULT 0.0,
    grid_export_kw    REAL     NOT NULL DEFAULT 0.0
)
"""

CREATE_TELEMETRY_INDEX = """
CREATE INDEX IF NOT EXISTS idx_telemetry_node_ts
    ON telemetry (node_id, timestamp DESC)
"""


# ---------------------------------------------------------------------------
# TelemetryReading — one MQTT payload / one SQLite row
# ---------------------------------------------------------------------------

@dataclass
class TelemetryReading:
    """
    Represents a single telemetry snapshot from a home node.

    Units
    -----
    voltage_v        : Volts
    current_a        : Amperes
    power_solar_kw   : kW (positive = generating)
    power_load_kw    : kW (positive = consuming)
    soc_pct          : Battery State-of-Charge 0–100 %
    battery_power_kw : kW  (+ve = charging, -ve = discharging)
    grid_import_kw   : kW bought from grid
    grid_export_kw   : kW sold to grid
    """
    node_id:          str
    timestamp:        str
    voltage_v:        float
    current_a:        float
    power_solar_kw:   float
    power_load_kw:    float
    soc_pct:          float
    battery_power_kw: float = 0.0
    grid_import_kw:   float = 0.0
    grid_export_kw:   float = 0.0

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------
    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, d: dict) -> "TelemetryReading":
        """
        Flexible constructor that also accepts the teammate's payload schema
        (solar_w / load_w in Watts, battery_soc) for backwards compatibility.
        """
        # Handle teammate's Watt-based fields (convert W → kW)
        solar_kw = d.get("power_solar_kw", d.get("solar_w", 0.0) / 1000.0)
        load_kw  = d.get("power_load_kw",  d.get("load_w",  0.0) / 1000.0)
        soc      = d.get("soc_pct",        d.get("battery_soc", 0.0))

        return cls(
            node_id          = d["node_id"],
            timestamp        = d["timestamp"],
            voltage_v        = float(d.get("voltage_v",        230.0)),
            current_a        = float(d.get("current_a",        0.0)),
            power_solar_kw   = float(solar_kw),
            power_load_kw    = float(load_kw),
            soc_pct          = float(soc),
            battery_power_kw = float(d.get("battery_power_kw", 0.0)),
            grid_import_kw   = float(d.get("grid_import_kw",   0.0)),
            grid_export_kw   = float(d.get("grid_export_kw",   0.0)),
        )

    @classmethod
    def from_json(cls, payload: str) -> "TelemetryReading":
        return cls.from_dict(json.loads(payload))

    @classmethod
    def from_sqlite_row(cls, row) -> "TelemetryReading":
        """Construct from a sqlite3.Row object (returned by EdgeDatabase queries)."""
        return cls(
            node_id          = row["node_id"],
            timestamp        = row["timestamp"],
            voltage_v        = row["voltage_v"],
            current_a        = row["current_a"],
            power_solar_kw   = row["power_solar_kw"],
            power_load_kw    = row["power_load_kw"],
            soc_pct          = row["soc_pct"],
            battery_power_kw = row["battery_power_kw"],
            grid_import_kw   = row["grid_import_kw"],
            grid_export_kw   = row["grid_export_kw"],
        )


# ---------------------------------------------------------------------------
# NodeSummary — the only data that ever leaves the edge node
# ---------------------------------------------------------------------------

@dataclass
class NodeSummary:
    """
    Anonymised / aggregated view of a node produced by EdgeDatabase.get_summary().
    This is the ONLY data structure that the Strategic LLM Agent
    and Tactical Orchestrator should consume from the edge layer.

    Granular sensor readings (voltage, current history) never leave the node.
    """
    node_id:        str
    as_of:          str          # ISO-8601 timestamp of most recent reading
    avg_load_kw:    float        # Average load over the summary window
    avg_solar_kw:   float        # Average solar generation
    current_soc_pct: float       # Latest battery SoC%
    net_energy_kw:  float        # avg_solar − avg_load  (+ve surplus, −ve deficit)
    intent:         str          # "SURPLUS" | "DEFICIT" | "BALANCED"
    sample_count:   int          # Number of readings in the summary window

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
