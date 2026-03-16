"""
orchestrator/safe_window.py
==========================
Publishes the physical constraint envelope (Safe Operating Window)
to the Strategic LLM Agent.
"""
import json
import logging
from dataclasses import asdict, dataclass
from typing import List

@dataclass
class SafeOperatingWindow:
    node_id: str
    timestamp: str
    state: str
    grid_status: str
    current_soc_pct: float
    available_discharge_kwh: float
    available_charge_kwh: float
    max_buy_p2p_kw: float
    max_sell_p2p_kw: float
    can_trade: bool
    constraints: List[str]

class SafeWindowPublisher:
    """
    Computes and MQTT-publishes current operational constraints.
    """
    def __init__(self, node_id: str, mqtt_client):
        self.node_id = node_id
        self._mqtt = mqtt_client
        
    def compute_and_publish(self, 
                           topic: str,
                           timestamp: str,
                           state: str, 
                           grid_status: str,
                           soc_pct: float,
                           available_discharge_kwh: float,
                           battery_cap_kwh: float) -> SafeOperatingWindow:
        
        # Logic for trading permissions
        can_trade = state in ["GRID_CONNECTED", "ISLANDED"]
        
        # Max P2P buy/sell limits (heuristic for demo)
        # Sell is limited by available discharge
        # Buy is limited by remaining battery capacity
        max_sell = min(3.0, available_discharge_kwh * 4) # Assume 15min interval = 4C rate max
        max_buy  = min(3.0, (battery_cap_kwh - (soc_pct/100.0 * battery_cap_kwh)) * 4)

        constraints = []
        if state == "EMERGENCY":
            constraints.append("EMERGENCY_LOAD_SHEDDING_ACTIVE")
            can_trade = False
        if soc_pct <= 10.0:
            constraints.append("DISCHARGE_BLOCKED_LOW_SOC")
            max_sell = 0.0
        if grid_status == "FAILED":
            constraints.append("GRID_UNAVAILABLE")

        window = SafeOperatingWindow(
            node_id=self.node_id,
            timestamp=timestamp,
            state=state,
            grid_status=grid_status,
            current_soc_pct=round(soc_pct, 1),
            available_discharge_kwh=round(available_discharge_kwh, 3),
            available_charge_kwh=round(battery_cap_kwh - (soc_pct/100.0 * battery_cap_kwh), 3),
            max_buy_p2p_kw=round(max_buy, 2),
            max_sell_p2p_kw=round(max_sell, 2),
            can_trade=can_trade,
            constraints=constraints
        )

        self._mqtt.publish(topic, json.dumps(asdict(window)), qos=1)
        return window
