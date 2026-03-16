"""
orchestrator/failover_manager.py
===============================
Detects grid instability or total failure based on voltage readings.
Includes a debounce mechanism to prevent flapping.
"""
import logging
from enum import Enum
from edge.config import VOLTAGE_UNSTABLE_V, VOLTAGE_FAILED_V, GRID_FAILURE_DEBOUNCE

logger = logging.getLogger("Orchestrator.Failover")

class GridStatus(Enum):
    CONNECTED = "CONNECTED"
    UNSTABLE = "UNSTABLE"
    FAILED = "FAILED"

class FailoverManager:
    """
    Analyzes telemetry to detect grid issues and suggest failover actions.
    """
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.failure_counter = 0
        self.last_status = GridStatus.CONNECTED
        
    def assess(self, voltage_v: float) -> GridStatus:
        """
        Evaluate grid health. Voltage below thresholds must persist for 
        GRID_FAILURE_DEBOUNCE readings to trigger a state change.
        """
        current_read = GridStatus.CONNECTED
        
        if voltage_v <= VOLTAGE_FAILED_V:
            current_read = GridStatus.FAILED
        elif voltage_v <= VOLTAGE_UNSTABLE_V:
            current_read = GridStatus.UNSTABLE
            
        # Debounce logic
        if current_read != GridStatus.CONNECTED:
            self.failure_counter += 1
            if self.failure_counter >= GRID_FAILURE_DEBOUNCE:
                self.last_status = current_read
        else:
            self.failure_counter = 0
            self.last_status = GridStatus.CONNECTED
            
        return self.last_status

    def is_healthy(self) -> bool:
        return self.last_status == GridStatus.CONNECTED
