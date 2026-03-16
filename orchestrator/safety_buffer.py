"""
orchestrator/safety_buffer.py
============================
Enforces the mandatory 10% SoC reserve and validates LLM commands against
physical constraints.
"""
import logging
from enum import Enum
from edge.config import SAFETY_BUFFER_SOC

logger = logging.getLogger("Orchestrator.Safety")

class SafetyVerdict(Enum):
    ALLOW = "ALLOW"
    BLOCK_DISCHARGE = "BLOCK_DISCHARGE"
    CRITICAL = "CRITICAL"

class SafetyBuffer:
    """
    Monitors Battery State-of-Charge and prevents deep discharge.
    """
    def __init__(self, node_id: str, buffer_soc: float = SAFETY_BUFFER_SOC):
        self.node_id = node_id
        self.buffer_soc = buffer_soc
        self.critical_soc = 5.0  # Absolute floor
        
    def check(self, soc_pct: float) -> SafetyVerdict:
        """
        Evaluate current SoC against safety thresholds.
        """
        if soc_pct <= self.critical_soc:
            return SafetyVerdict.CRITICAL
        elif soc_pct <= self.buffer_soc:
            return SafetyVerdict.BLOCK_DISCHARGE
        return SafetyVerdict.ALLOW

    def validate_llm_command(self, command: dict, current_soc: float) -> tuple[bool, str]:
        """
        Validate an incoming LLM command against safety rules.
        Overrides SELL/DISCHARGE if it would breach the buffer.
        """
        action = command.get("action", "").upper()
        
        if action in ["SELL", "DISCHARGE"]:
            verdict = self.check(current_soc)
            if verdict != SafetyVerdict.ALLOW:
                reason = f"Command {action} rejected: SoC ({current_soc}%) is at or below safety buffer ({self.buffer_soc}%)."
                logger.warning(f"[{self.node_id}] {reason}")
                return False, reason
                
        return True, "Approved"

    def get_available_capacity_kwh(self, current_soc: float, total_capacity_kwh: float) -> float:
        """
        Calculate usable energy above the safety buffer.
        """
        usable_soc = max(0.0, current_soc - self.buffer_soc)
        return round((usable_soc / 100.0) * total_capacity_kwh, 4)
