"""
orchestrator/orchestrator.py
============================
The main Tactical Orchestrator entry point.
Subscribes to telemetry, manages FSM, enforces safety, and handles LLM commands.
"""
import json
import logging
import threading
from typing import Optional

import paho.mqtt.client as mqtt

from edge import config
from edge.models import TelemetryReading
from orchestrator.fsm import MicrogridFSM
from orchestrator.safety_buffer import SafetyBuffer, SafetyVerdict
from orchestrator.failover_manager import FailoverManager, GridStatus
from orchestrator.mqtt_handshake import MQTTHandshake, HandshakeResult
from orchestrator.safe_window import SafeWindowPublisher

logger = logging.getLogger("Orchestrator.Main")

class TacticalOrchestrator:
    """
    Coordinates sub-second safety and operational logic for one home node.
    """
    def __init__(self, node_id: str, edge_node):
        self.node_id = node_id
        self.edge_node = edge_node  # edge.EdgeNode instance
        
        # Sub-modules
        self.fsm = MicrogridFSM(node_id)
        self.safety = SafetyBuffer(node_id)
        self.failover = FailoverManager(node_id)
        
        # MQTT Topics
        self.topic_telemetry = config.telemetry_topic(node_id)
        self.topic_llm_cmds  = config.llm_commands_topic(node_id)
        self.topic_safe_win  = config.safe_window_topic(node_id)
        self.topic_hs_req    = config.handshake_request_topic(node_id)
        self.topic_hs_res    = config.handshake_response_topic(node_id)

        # MQTT Client (internal for orchestrator control loop)
        self._client = mqtt.Client(client_id=f"Orchestrator_{node_id}")
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        
        self.handshake = MQTTHandshake(node_id, self._client)
        self.publisher = SafeWindowPublisher(node_id, self._client)

    # ------------------------------------------------------------------
    # MQTT Callbacks
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe([(self.topic_telemetry, 1), 
                              (self.topic_llm_cmds, 1),
                              (self.topic_hs_req, 1),
                              (self.topic_hs_res, 1)])
            logger.info(f"[{self.node_id}] Orchestrator connected and subscribed to topics.")
        else:
            logger.error(f"[{self.node_id}] Orchestrator connection failed (rc={rc}).")

    def _on_message(self, client, userdata, msg):
        """Dispatches incoming MQTT messages to handlers."""
        try:
            payload = msg.payload.decode("utf-8")
            
            if msg.topic == self.topic_telemetry:
                self._handle_telemetry(payload)
            elif msg.topic == self.topic_llm_cmds:
                threading.Thread(target=self._handle_llm_command, args=(payload,), daemon=True).start()
            elif msg.topic == self.topic_hs_req:
                self._handle_handshake_request(payload)
            elif msg.topic == self.topic_hs_res:
                self.handshake.handle_response(json.loads(payload))
                
        except Exception as e:
            logger.error(f"[{self.node_id}] Error in orchestrator msg handler: {e}")

    # ------------------------------------------------------------------
    # Logic Handlers
    # ------------------------------------------------------------------
    def _handle_telemetry(self, raw_json: str):
        """Processes real-time telemetry and updates FSM/Safety status."""
        reading = TelemetryReading.from_json(raw_json)
        
        # 1. Check Safety
        verdict = self.safety.check(reading.soc_pct)
        if verdict == SafetyVerdict.CRITICAL and self.fsm.state != "EMERGENCY":
            self.fsm.critical_soc()
            
        # 2. Check Grid Failover
        grid_status = self.failover.assess(reading.voltage_v)
        if grid_status == GridStatus.FAILED and self.fsm.state in ["GRID_CONNECTED", "P2P_TRADING"]:
            self.fsm.grid_failed()
        elif grid_status == GridStatus.CONNECTED and self.fsm.state == "ISLANDED":
            self.fsm.grid_restored()

        # 3. Publish Safe Operating Window
        avail_discharge = self.safety.get_available_capacity_kwh(
            reading.soc_pct, self.edge_node.battery_capacity_kwh
        )
        self.publisher.compute_and_publish(
            topic=self.topic_safe_win,
            timestamp=reading.timestamp,
            state=self.fsm.state,
            grid_status=grid_status.value,
            soc_pct=reading.soc_pct,
            available_discharge_kwh=avail_discharge,
            battery_cap_kwh=self.edge_node.battery_capacity_kwh
        )

    def _handle_llm_command(self, raw_json: str):
        """Validated and executes high-level commands from the LLM agent."""
        try:
            cmd = json.loads(raw_json)
            logger.info(f"[{self.node_id}] Received LLM Command: {cmd}")
            
            # Safety Gate
            current_soc = self.edge_node.get_latest_reading().soc_pct if self.edge_node.get_latest_reading() else 0.0
            ok, reason = self.safety.validate_llm_command(cmd, current_soc)
            if not ok:
                logger.error(f"[{self.node_id}] LLM Command REJECTED: {reason}")
                return

            # Execution logic (Handshake for trades)
            action = cmd.get("action", "").upper()
            if action in ["BUY", "SELL"]:
                target_peer = cmd.get("target")
                amount = cmd.get("amount_kwh", 0.0)
                price = cmd.get("price_per_kwh", 0.0)
                
                if not target_peer:
                    logger.error(f"[{self.node_id}] Buy/Sell command missing target peer.")
                    return
                
                # Perform Handshake
                self.fsm.start_trade()
                result = self.handshake.initiate(target_peer, amount, price)
                
                if result == HandshakeResult.ACCEPTED:
                    logger.info(f"[{self.node_id}] Trade finalized with {target_peer}. Opening simulated circuits...")
                    # In Phase 5/6, this would trigger relay switching in Pandapower.
                else:
                    logger.warning(f"[{self.node_id}] Trade failed ({result}).")
                
                self.fsm.finish_trade()

        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to process LLM command: {e}")

    def _handle_handshake_request(self, raw_json: str):
        """Automatically respond to peer handshake requests based on local state."""
        try:
            req = json.loads(raw_json)
            sender = req["sender_id"]
            
            # Simple policy: accept if not in EMERGENCY and SoC is above buffer
            current_soc = self.edge_node.get_latest_reading().soc_pct if self.edge_node.get_latest_reading() else 0.0
            verdict = self.safety.check(current_soc)
            
            if self.fsm.state == "EMERGENCY" or verdict != SafetyVerdict.ALLOW:
                self.handshake.send_response(req, HandshakeResult.REJECTED)
            else:
                self.handshake.send_response(req, HandshakeResult.ACCEPTED)
                
        except Exception as e:
            logger.error(f"[{self.node_id}] Error handling handshake request: {e}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self, broker_host=config.MQTT_BROKER, broker_port=config.MQTT_PORT):
        """Starts the Orchestrator loop."""
        logger.info(f"[{self.node_id}] Starting Tactical Orchestrator...")
        self._client.connect(broker_host, broker_port, keepalive=60)
        self._client.loop_start()

    def stop(self):
        """Stops the Orchestrator loop."""
        self._client.loop_stop()
        self._client.disconnect()
        logger.info(f"[{self.node_id}] Orchestrator stopped.")
