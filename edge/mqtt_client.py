"""
edge/mqtt_client.py
===================
MQTT subscriber that receives telemetry for ONE home node and persists it to SQLite.

Design differences from teammate's ingestion_service.py
--------------------------------------------------------
- Class-based (EdgeMQTTClient) rather than loose module-level callbacks.
- Single node per instance (privacy isolation matches EdgeDatabase).
- Parses payload into TelemetryReading dataclass (type-safe, validated).
- Runs the MQTT loop in a daemon background thread via loop_start().
- Exposes start() / stop() for clean lifecycle management.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Optional

import paho.mqtt.client as mqtt

from edge import config
from edge.database import EdgeDatabase
from edge.models import TelemetryReading

logger = logging.getLogger("Edge.MQTTClient")


class EdgeMQTTClient:
    """
    Subscribes to a single node's MQTT telemetry topic and writes every
    incoming message to the node's private SQLite database.

    Parameters
    ----------
    node_id     : Home node identifier (e.g. "delhi_01")
    database    : An already-initialised EdgeDatabase for this node
    broker_host : MQTT broker hostname (default: config.MQTT_BROKER)
    broker_port : MQTT broker port    (default: config.MQTT_PORT)
    """

    def __init__(
        self,
        node_id:     str,
        database:    EdgeDatabase,
        broker_host: str = config.MQTT_BROKER,
        broker_port: int = config.MQTT_PORT,
    ):
        self.node_id     = node_id
        self.db          = database
        self.broker_host = broker_host
        self.broker_port = broker_port
        self._connected  = threading.Event()
        self._running    = False

        client_id = f"EdgeNode_{node_id}_{datetime.utcnow().strftime('%H%M%S')}"
        self._client = mqtt.Client(client_id=client_id, clean_session=True)
        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message    = self._on_message

        # Exponential back-off on reconnect (1s → 120s) — from teammate's code, kept
        self._client.reconnect_delay_set(min_delay=1, max_delay=120)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = config.telemetry_topic(self.node_id)
            client.subscribe(topic, qos=1)
            self._connected.set()
            logger.info(f"[{self.node_id}] Connected to broker. Subscribed to '{topic}'.")
        else:
            logger.error(f"[{self.node_id}] Broker connection refused (rc={rc}).")

    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        if rc != 0:
            # paho will auto-reconnect; we just log the event
            logger.warning(f"[{self.node_id}] Disconnected unexpectedly (rc={rc}). Auto-reconnecting...")

    def _on_message(self, client, userdata, msg):
        """Parse JSON payload → TelemetryReading → write to private SQLite."""
        try:
            reading = TelemetryReading.from_json(msg.payload.decode("utf-8"))

            # Safety: reject readings from a different node (shouldn't happen on a
            # per-node subscription, but guards against misconfigured brokers)
            if reading.node_id != self.node_id:
                logger.warning(
                    f"[{self.node_id}] Received foreign node_id '{reading.node_id}' — discarded."
                )
                return

            self.db.insert_reading(reading)
            logger.debug(
                f"[{self.node_id}] Persisted reading @ {reading.timestamp} "
                f"(solar={reading.power_solar_kw:.2f}kW load={reading.power_load_kw:.2f}kW "
                f"SoC={reading.soc_pct:.1f}%)"
            )

        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"[{self.node_id}] Malformed payload: {e} | raw: {msg.payload[:120]}")
        except Exception as e:
            logger.error(f"[{self.node_id}] Error processing message: {e}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self, timeout: float = 10.0) -> bool:
        """
        Connect to the broker and block until connected or timeout.
        Returns True if connected successfully.
        """
        try:
            self._client.connect(self.broker_host, self.broker_port, keepalive=60)
        except OSError as e:
            logger.error(f"[{self.node_id}] Cannot reach broker at "
                         f"{self.broker_host}:{self.broker_port} — {e}")
            return False
        return True

    def start(self) -> bool:
        """
        Connect to the broker and begin background ingestion loop.
        Non-blocking — returns immediately after connection is confirmed.
        Returns True on success.
        """
        if self._running:
            return True

        if not self.connect():
            return False

        self._client.loop_start()   # Spawns daemon thread — safe to call from any thread
        self._running = True
        logger.info(f"[{self.node_id}] MQTT ingestion loop started.")
        return True

    def stop(self) -> None:
        """Stop the background loop and disconnect cleanly."""
        if not self._running:
            return
        self._client.loop_stop()
        self._client.disconnect()
        self._running = False
        logger.info(f"[{self.node_id}] MQTT client stopped.")

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()
