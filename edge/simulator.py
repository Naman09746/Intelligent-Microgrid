"""
edge/simulator.py
=================
Microgrid telemetry simulator — generates and MQTT-publishes realistic sensor
readings for ALL 5 home nodes simultaneously.

Solar / load physics are adapted from the teammate's sensor_simulator.py
(which had good math) but expanded to:
  - Support all 5 NODE_CONFIGS at once (not just one hardcoded home_id)
  - Use kW units (matching TelemetryReading / EdgeDatabase schema)
  - Derive battery_power_kw, grid_import_kw, grid_export_kw
  - Allow configurable interval and start time
  - Proper OOP design (MicrogridSimulator class)
"""
from __future__ import annotations

import json
import logging
import math
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import paho.mqtt.client as mqtt

from edge import config
from edge.config import NODE_CONFIGS, TELEMETRY_INTERVAL
from edge.models import TelemetryReading

logger = logging.getLogger("Edge.Simulator")


def _simulate_solar_kw(hour_decimal: float, peak_kw: float = 3.0, seed_noise: float = 1.0) -> float:
    """
    Bell-curve solar generation matching the Solar Forecaster's physics.
    Active between 06:00 and 18:00 only.
    """
    if 6.0 <= hour_decimal <= 18.0:
        x = (hour_decimal - 6.0) / 12.0 * math.pi
        cloud_factor = random.uniform(0.88, 1.0) * seed_noise
        return round(math.sin(x) * peak_kw * cloud_factor, 3)
    return 0.0


def _simulate_load_kw(hour_decimal: float, rng: random.Random) -> float:
    """
    Double-peak residential load profile (Load Forecaster pattern):
      - Morning peak  07:00–09:00
      - Evening peak  18:00–21:00
      - Night baseline otherwise
    """
    base = rng.uniform(0.2, 0.4)
    if 7.0 <= hour_decimal < 9.0:
        return round(base + rng.uniform(1.0, 2.5), 3)
    elif 18.0 <= hour_decimal < 21.0:
        return round(base + rng.uniform(1.5, 3.5), 3)
    return round(base, 3)


class MicrogridSimulator:
    """
    Continuously generates synthetic telemetry for all 5 home nodes and
    publishes each reading to its dedicated MQTT topic.

    Parameters
    ----------
    broker_host : MQTT broker hostname
    broker_port : MQTT broker port
    interval    : Seconds between publish ticks (default from config)
    start_time  : Simulation start time (default: current UTC time)
    time_step   : Simulated minutes advanced per real-time interval
                  (e.g. 15 means 1 tick = 15 simulated minutes)
    """

    def __init__(
        self,
        broker_host:  str = config.MQTT_BROKER,
        broker_port:  int = config.MQTT_PORT,
        interval:     int = TELEMETRY_INTERVAL,
        start_time:   Optional[datetime] = None,
        time_step_min: int = 1,
    ):
        self.broker_host   = broker_host
        self.broker_port   = broker_port
        self.interval      = interval
        self.time_step_min = time_step_min
        self._running      = False

        # Simulation clock — defaults to 00:00 today so we see a full day cycle
        self._sim_time = start_time or datetime.utcnow().replace(hour=0, minute=0, second=0)

        # Per-node persistent state (SoC evolves across ticks)
        self._node_state: Dict[str, dict] = {
            node_id: {
                "soc_pct":  random.uniform(40.0, 70.0),   # Realistic starting SoC
                "rng":      random.Random(hash(node_id)),  # Deterministic per-node noise
            }
            for node_id in NODE_CONFIGS
        }

        # MQTT client (one shared client publishes for all nodes)
        self._client = mqtt.Client(client_id="MicrogridSimulator", clean_session=True)
        self._client.on_connect = self._on_connect

    # ------------------------------------------------------------------
    # MQTT callbacks
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Simulator connected to broker at {self.broker_host}:{self.broker_port}.")
        else:
            logger.error(f"Simulator broker connection failed (rc={rc}).")

    # ------------------------------------------------------------------
    # Per-node reading generation
    # ------------------------------------------------------------------
    def _generate_reading(self, node_id: str, node_cfg: dict) -> TelemetryReading:
        """Produce one realistic TelemetryReading for the given node."""
        state = self._node_state[node_id]
        rng   = state["rng"]
        hour_dec = self._sim_time.hour + self._sim_time.minute / 60.0

        # Solar & load (kW)
        solar_kw = _simulate_solar_kw(hour_dec, peak_kw=3.0, seed_noise=rng.uniform(0.88, 1.0))
        load_kw  = _simulate_load_kw(hour_dec, rng)

        # Battery kinetics: net = solar − load over this time step
        capacity_kwh = node_cfg["battery_capacity_wh"] / 1000.0
        step_h       = self.time_step_min / 60.0
        net_kwh      = (solar_kw - load_kw) * step_h

        soc_before = state["soc_pct"]
        delta_soc  = (net_kwh / capacity_kwh) * 100.0
        new_soc    = max(0.0, min(100.0, soc_before + delta_soc))
        state["soc_pct"] = new_soc

        battery_power_kw = solar_kw - load_kw         # +ve = charging
        grid_import_kw   = max(0.0, load_kw - solar_kw - max(0.0, battery_power_kw))
        grid_export_kw   = max(0.0, solar_kw - load_kw - max(0.0, -battery_power_kw))

        voltage_v = round(rng.uniform(225.0, 235.0), 1)
        current_a = round(load_kw * 1000.0 / voltage_v, 2) if voltage_v > 0 else 0.0

        return TelemetryReading(
            node_id          = node_id,
            timestamp        = self._sim_time.strftime("%Y-%m-%dT%H:%M:%S"),
            voltage_v        = voltage_v,
            current_a        = current_a,
            power_solar_kw   = solar_kw,
            power_load_kw    = round(load_kw, 3),
            soc_pct          = round(new_soc, 1),
            battery_power_kw = round(battery_power_kw, 3),
            grid_import_kw   = round(grid_import_kw, 3),
            grid_export_kw   = round(grid_export_kw, 3),
        )

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------
    def publish_all(self) -> None:
        """Generate and publish one reading for every node in NODE_CONFIGS."""
        for node_id, node_cfg in NODE_CONFIGS.items():
            reading = self._generate_reading(node_id, node_cfg)
            topic   = config.telemetry_topic(node_id)
            payload = reading.to_json()
            result  = self._client.publish(topic, payload, qos=1)

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(
                    f"[{node_id}] {reading.timestamp} | "
                    f"solar={reading.power_solar_kw:.2f}kW  "
                    f"load={reading.power_load_kw:.2f}kW  "
                    f"SoC={reading.soc_pct:.1f}%"
                )
            else:
                logger.warning(f"[{node_id}] Publish failed (rc={result.rc})")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> bool:
        """Connect to broker. Returns True if successful."""
        try:
            self._client.connect(self.broker_host, self.broker_port, keepalive=60)
        except OSError as e:
            logger.error(f"Cannot connect to broker at {self.broker_host}:{self.broker_port} — {e}")
            return False
        self._client.loop_start()
        return True

    def run(self, interval: Optional[int] = None, ticks: Optional[int] = None) -> None:
        """
        Continuously publish telemetry for all nodes.

        Parameters
        ----------
        interval : Seconds between ticks (overrides constructor value if given)
        ticks    : If set, stop after this many publish cycles (useful for tests/demos)
        """
        tick_interval = interval or self.interval
        if not self.start():
            return

        self._running = True
        tick_count    = 0

        logger.info(
            f"Simulator running — {len(NODE_CONFIGS)} nodes, "
            f"{tick_interval}s real-time interval, "
            f"{self.time_step_min}min simulated step per tick."
        )

        try:
            while self._running:
                self.publish_all()
                # Advance simulation clock
                self._sim_time += timedelta(minutes=self.time_step_min)
                tick_count += 1

                if ticks is not None and tick_count >= ticks:
                    logger.info(f"Completed {ticks} ticks. Stopping.")
                    break

                time.sleep(tick_interval)
        except KeyboardInterrupt:
            logger.info("Simulator interrupted.")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop publishing and disconnect cleanly."""
        self._running = False
        self._client.loop_stop()
        self._client.disconnect()
        logger.info("Simulator stopped.")
