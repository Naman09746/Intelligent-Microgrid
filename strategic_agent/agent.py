"""
strategic_agent/agent.py
=======================
The main Strategic Agent reasoning loop.
"""
import logging
import json
import threading
import time
from typing import Optional, Dict, Any, List

import paho.mqtt.client as mqtt

from edge import config
from edge.node import EdgeNode
from strategic_agent.llm_client import GeminiClient
from strategic_agent.prompt_builder import PromptBuilder
from strategic_agent.command_parser import CommandParser, AgentCommand
from strategic_agent.negotiation import MarketplaceClient

logger = logging.getLogger("StrategicAgent.Main")

class StrategicAgent:
    """
    Orchestrates the reasoning cycle: Data Gathering -> LLM Reasoning -> Action.
    """
    def __init__(self, 
                 node_id: str, 
                 edge_node: EdgeNode,
                 llm_client: GeminiClient,
                 marketplace: MarketplaceClient):
        
        self.node_id = node_id
        self.edge_node = edge_node
        self.llm = llm_client
        self.marketplace = marketplace
        
        # Internal modules
        self.builder = PromptBuilder()
        self.parser = CommandParser()
        
        # State tracking
        self._last_safe_window: Dict[str, Any] = {}
        self._cycle_count = 0
        self._is_running = False
        self._thread: Optional[threading.Thread] = None

        # MQTT for Safe Window subscription and Command publishing
        self._mqtt = mqtt.Client(client_id=f"StrategicAgent_{node_id}")
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        
        self.topic_safe_window = config.safe_window_topic(node_id)
        self.topic_commands = config.llm_commands_topic(node_id)

    # ------------------------------------------------------------------
    # MQTT Callbacks
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(self.topic_safe_window)
            logger.info(f"[{self.node_id}] Strategic Agent subscribed to safe window.")
        else:
            logger.error(f"[{self.node_id}] MQTT connection failed (rc={rc})")

    def _on_message(self, client, userdata, msg):
        try:
            if msg.topic == self.topic_safe_window:
                self._last_safe_window = json.loads(msg.payload.decode())
        except Exception as e:
            logger.error(f"Error handling safe window update: {e}")

    # ------------------------------------------------------------------
    # Reasoning Cycle
    # ------------------------------------------------------------------
    def _apply_guardrails(self, cmd: AgentCommand, snapshot_soc: float) -> AgentCommand:
        """Clamp obviously unsafe strategic actions before dispatch to tactical layer."""
        if snapshot_soc >= 98.0 and cmd.action in ["BUY", "CHARGE"]:
            requested_action = cmd.action
            cmd.action = "HOLD"
            cmd.amount_kwh = 0.0
            cmd.price_per_kwh = 0.0
            cmd.target = "battery"
            cmd.reasoning = (
                f"Guardrail override: SoC={snapshot_soc:.1f}% near full, "
                f"blocked {requested_action} request."
            )

        if snapshot_soc <= config.SAFETY_BUFFER_SOC and cmd.action in ["SELL", "DISCHARGE"]:
            cmd.action = "HOLD"
            cmd.amount_kwh = 0.0
            cmd.price_per_kwh = 0.0
            cmd.target = "battery"
            cmd.reasoning = (
                f"Guardrail override: SoC={snapshot_soc:.1f}% at/below safety buffer, "
                f"blocked discharge-type action."
            )

        if not self._last_safe_window.get("can_trade", True) and cmd.action in ["BUY", "SELL"]:
            cmd.action = "HOLD"
            cmd.amount_kwh = 0.0
            cmd.price_per_kwh = 0.0
            cmd.target = "battery"
            cmd.reasoning = "Guardrail override: current safe window forbids trading."

        return cmd

    def run_cycle(self) -> Optional[AgentCommand]:
        """Performs one full reasoning loop."""
        logger.info(f"[{self.node_id}] Starting reasoning cycle...")

        trace_topic_f = f"dashboard/trace/{self.node_id}/forecast"
        trace_topic_a = f"dashboard/trace/{self.node_id}/agent"
        
        # 1. Gather Node Status
        status = self.edge_node.get_status(hours=1)
        if not status:
            logger.warning("No node status available yet. Skipping cycle.")
            now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            self._mqtt.publish(trace_topic_f, json.dumps({
                "input": "Waiting for telemetry batch...",
                "output": {"load": [], "solar": []},
                "ts": now
            }))
            self._mqtt.publish(trace_topic_a, json.dumps({
                "input": "Node status unavailable",
                "reasoning": "Waiting for ingested telemetry before strategic reasoning.",
                "output": {
                    "action": "HOLD",
                    "amount_kwh": 0.0,
                    "price_per_kwh": 0.0,
                    "target": "grid",
                    "snapshot_soc": None
                },
                "ts": now
            }))
            return None
        node_data = status.to_dict()
        snapshot_soc = status.current_soc_pct # Freeze the SOC the LLM will reason about

        # 2. Market Snapshot
        market_data = self.marketplace.get_market_snapshot()
        grid_prices = {"buy": 8.50, "sell": 3.00} # Default grid prices

        # 3. Forecasts (Dynamic Rolling Mocks)
        # We shift the 24h curves based on the current simulated hour
        import random
        from datetime import datetime
        
        as_of_str = node_data.get('as_of', '')
        logger.info(f"[{self.node_id}] Simulation Time (as_of): {as_of_str}")
        
        try:
            # Robust parsing: handles T or space, and ignores timezone offsets if problematic
            clean_ts = as_of_str.replace(' ', 'T').split('.')[0]
            current_hour = datetime.fromisoformat(clean_ts).hour
        except (ValueError, TypeError, IndexError) as e:
            logger.warning(f"Failed to parse 'as_of' timestamp '{as_of_str}': {e}. Defaulting to 0.")
            current_hour = 0
            
        # 24-hour baseline profiles
        base_load = [0.4, 0.3, 0.3, 0.4, 0.6, 1.2, 2.5, 3.2, 2.8, 1.5, 1.2, 1.0, 
                     0.8, 0.7, 0.6, 0.5, 0.8, 1.5, 2.8, 3.5, 3.0, 1.5, 0.8, 0.5]
        base_solar = [0, 0, 0, 0, 0, 0, 0.5, 1.2, 2.0, 2.8, 3.2, 3.4, 
                      3.3, 2.8, 1.8, 0.8, 0.2, 0, 0, 0, 0, 0, 0, 0]
                      
        # Roll them so index 0 corresponds to current_hour
        def roll_and_noisfy(arr, shift, noise_level=0.2):
            rolled = arr[shift:] + arr[:shift]
            return [round(max(0, x * random.uniform(1-noise_level, 1+noise_level)), 3) for x in rolled]

        mock_load  = roll_and_noisfy(base_load, current_hour, noise_level=0.3)
        mock_solar = roll_and_noisfy(base_solar, current_hour, noise_level=0.1)

        # 4. Trade History
        history = self.marketplace.get_node_trades(self.node_id, limit=5)

        # DASHBOARD TRACE: Phase 2 Forecasting (Mocked features)
        self._mqtt.publish(trace_topic_f, json.dumps({
            "input": f"Feature Vector: [SoC={node_data.get('current_soc_pct')}, Load={node_data.get('avg_load_kw')}, ...]",
            "output": {
                "load": mock_load[:8],
                "solar": mock_solar[:8],
                "start_hour": current_hour
            },
            "ts": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        }))

        # 5. Build Prompt
        self._cycle_count += 1
        prompt = self.builder.build(
            node_id=self.node_id,
            node_status=node_data,
            safe_window=self._last_safe_window,
            market_snapshot=market_data,
            load_forecast=mock_load,
            solar_forecast=mock_solar,
            grid_prices=grid_prices,
            trade_history=history,
            cycle_id=self._cycle_count
        )
        
        logger.info(f"[{self.node_id}] Sent prompt for Cycle {self._cycle_count}. Snapshot SoC: {snapshot_soc}%")
        logger.debug(f"FULL PROMPT: {prompt}")

        # 6. LLM Inference
        llm_response = self.llm.infer_json(prompt, schema=self.llm.response_schema)
        
        # 7. Parse Action
        cmd = self.parser.parse(llm_response)
        cmd = self._apply_guardrails(cmd, snapshot_soc)
        logger.info(f"[{self.node_id}] LLM Decision: {cmd.action} {cmd.amount_kwh}kWh @ ₹{cmd.price_per_kwh}")
        logger.info(f"[{self.node_id}] Reasoning: {cmd.reasoning}")

        # 8. Execute Action (Marketplace and Orchestrator)
        if cmd.action in ["BUY", "SELL"]:
            # Place order on marketplace first
            order_result = self.marketplace.place_order(
                node_id=self.node_id,
                order_type=cmd.action,
                quantity_kwh=cmd.amount_kwh,
                price_per_kwh=cmd.price_per_kwh
            )
            
            # If the market matched us immediately, the trade is recorded.
            # Regardless, notify the orchestrator to prepare for physical handshake.
            cmd_json = self.parser.to_orchestrator_json(cmd, snapshot_soc=snapshot_soc)
            self._mqtt.publish(self.topic_commands, cmd_json, qos=1)
            
        elif cmd.action in ["CHARGE", "DISCHARGE"]:
            # Direct battery control via orchestrator
            cmd_json = self.parser.to_orchestrator_json(cmd, snapshot_soc=snapshot_soc)
            self._mqtt.publish(self.topic_commands, cmd_json, qos=1)

        else:
            # Forward HOLD to orchestrator so the dashboard reflects the agent's decision
            cmd_json = self.parser.to_orchestrator_json(cmd, snapshot_soc=snapshot_soc)
            self._mqtt.publish(self.topic_commands, cmd_json, qos=1)

        # DASHBOARD TRACE: Publish inputs, reasoning, and final command
        self._mqtt.publish(trace_topic_a, json.dumps({
            "input": prompt[:200] + "...", # Summarized for dashboard
            "reasoning": cmd.reasoning,
            "output": {
                "action": cmd.action,
                "amount_kwh": cmd.amount_kwh,
                "price_per_kwh": cmd.price_per_kwh,
                "target": cmd.target,
                "snapshot_soc": snapshot_soc,
                "current_soc": node_data.get('current_soc_pct')
            },
            "ts": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        }))

        return cmd

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self, interval_seconds: int = 600):
        if self._is_running: return
        
        self._is_running = True
        self._mqtt.connect(config.MQTT_BROKER, config.MQTT_PORT)
        self._mqtt.loop_start()
        
        def loop():
            while self._is_running:
                try:
                    self.run_cycle()
                except Exception as e:
                    logger.error(f"Error in reasoning cycle: {e}")
                time.sleep(interval_seconds)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        logger.info(f"[{self.node_id}] Strategic Agent loop started (interval={interval_seconds}s)")

    def stop(self):
        self._is_running = False
        self._mqtt.loop_stop()
        self._mqtt.disconnect()
        if self._thread:
            self._thread.join(timeout=2)
        logger.info(f"[{self.node_id}] Strategic Agent loop stopped.")
