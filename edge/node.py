"""
edge/node.py
============
Top-level EdgeNode class — wires EdgeDatabase + EdgeMQTTClient together
for a single home node.

This is the object that Phase 3 (Tactical Orchestrator) and Phase 4
(Strategic LLM Agent) will import and call.  They never touch the database
or MQTT directly; they go through EdgeNode.get_status() / get_history().
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from edge import config
from edge.config import NODE_CONFIGS
from edge.database import EdgeDatabase
from edge.models import NodeSummary, TelemetryReading
from edge.mqtt_client import EdgeMQTTClient

logger = logging.getLogger("Edge.Node")


class EdgeNode:
    """
    Orchestrates the full edge data pipeline for one home node:
      Database initialisation → MQTT subscription → real-time ingestion → query API.

    Parameters
    ----------
    node_id     : One of the keys in NODE_CONFIGS (e.g. "delhi_01")
    broker_host : MQTT broker hostname
    broker_port : MQTT broker port
    db_dir      : Directory for per-node SQLite files
    """

    def __init__(
        self,
        node_id:     str,
        broker_host: str = config.MQTT_BROKER,
        broker_port: int = config.MQTT_PORT,
        db_dir:      str = config.DB_DIR,
    ):
        if node_id not in NODE_CONFIGS:
            raise ValueError(
                f"Unknown node_id '{node_id}'. "
                f"Valid ids: {list(NODE_CONFIGS.keys())}"
            )

        self.node_id   = node_id
        self.node_cfg  = NODE_CONFIGS[node_id]
        self._db       = EdgeDatabase(node_id, db_dir=db_dir)
        self._mqtt     = EdgeMQTTClient(node_id, self._db, broker_host, broker_port)
        self._started  = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> bool:
        """
        Initialise the database schema and begin MQTT ingestion.
        Returns True if the MQTT connection succeeded.
        """
        self._db.initialize()
        ok = self._mqtt.start()
        if ok:
            self._started = True
            logger.info(
                f"[{self.node_id}] EdgeNode started "
                f"(city={self.node_cfg['city']}, "
                f"battery={self.node_cfg['battery_capacity_wh']/1000:.0f}kWh)."
            )
        else:
            logger.error(f"[{self.node_id}] EdgeNode could not connect to MQTT broker.")
        return ok

    def stop(self) -> None:
        """Cleanly shut down MQTT and close the database."""
        self._mqtt.stop()
        self._db.close()
        self._started = False
        logger.info(f"[{self.node_id}] EdgeNode stopped.")

    # ------------------------------------------------------------------
    # Query API  (used by Tactical Orchestrator and Strategic LLM Agent)
    # ------------------------------------------------------------------
    def get_status(self, hours: int = 1) -> Optional[NodeSummary]:
        """
        Return a NodeSummary for the last `hours` of ingested data.
        Returns None if the database has no data yet.
        """
        return self._db.get_summary(hours=hours)

    def get_latest_reading(self) -> Optional[TelemetryReading]:
        """Return the single most recent raw TelemetryReading."""
        readings = self._db.get_latest(1)
        return readings[0] if readings else None

    def get_history(self, hours: int = 24) -> pd.DataFrame:
        """
        Return recent telemetry as a Pandas DataFrame for forecaster consumption.

        Parameters
        ----------
        hours : How many hours of history to retrieve (default: 24)

        Returns
        -------
        DataFrame with columns matching TelemetryReading fields, indexed by timestamp.
        Returns an empty DataFrame if no data exists.
        """
        end   = datetime.utcnow()
        start = end.replace(hour=0, minute=0, second=0) if hours >= 24 else end
        from datetime import timedelta
        readings = self._db.get_range(end - timedelta(hours=hours), end)

        if not readings:
            return pd.DataFrame()

        rows = [r.to_dict() for r in readings]
        df   = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        return df

    def run_maintenance(self) -> None:
        """Delete old readings beyond the configured retention window."""
        self._db.cleanup()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    @property
    def is_connected(self) -> bool:
        return self._mqtt.is_connected

    @property
    def city(self) -> str:
        return self.node_cfg["city"]

    @property
    def battery_capacity_kwh(self) -> float:
        return self.node_cfg["battery_capacity_wh"] / 1000.0

    def __repr__(self) -> str:
        status = "running" if self._started else "stopped"
        return f"<EdgeNode id={self.node_id} city={self.city} status={status}>"
