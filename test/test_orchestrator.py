"""
tests/test_orchestrator.py
==========================
Unit tests for the Tactical Orchestrator components.
"""
import pytest
from unittest.mock import MagicMock
from orchestrator.fsm import MicrogridFSM
from orchestrator.safety_buffer import SafetyBuffer, SafetyVerdict
from orchestrator.failover_manager import FailoverManager, GridStatus
from orchestrator.mqtt_handshake import MQTTHandshake, HandshakeResult

# ------------------------------------------------------------------
# FSM Tests
# ------------------------------------------------------------------
def test_fsm_initial_state_is_grid_connected():
    """Verify that the system starts in GRID_CONNECTED mode by default."""
    fsm = MicrogridFSM("test_node")
    assert fsm.state == "GRID_CONNECTED"

def test_fsm_handles_grid_failure_and_recovery():
    """Verify that the system transitions to ISLANDED during an outage and recovers safely."""
    fsm = MicrogridFSM("test_node")
    fsm.grid_failed()
    assert fsm.state == "ISLANDED"
    fsm.grid_restored()
    assert fsm.state == "GRID_CONNECTED"

def test_fsm_enters_emergency_on_critical_battery():
    """Verify that the system locks into EMERGENCY mode when battery is critically low."""
    fsm = MicrogridFSM("test_node")
    fsm.critical_soc()
    assert fsm.state == "EMERGENCY"

# ------------------------------------------------------------------
# Safety Buffer Tests
# ------------------------------------------------------------------
def test_safety_buffer_correctly_verdicts_soc_levels():
    """Ensure safety governor blocks discharge at 10% and flags critical at 5%."""
    sb = SafetyBuffer("test_node", buffer_soc=10.0)
    assert sb.check(50.0) == SafetyVerdict.ALLOW
    assert sb.check(10.0) == SafetyVerdict.BLOCK_DISCHARGE
    assert sb.check(5.0) == SafetyVerdict.CRITICAL

def test_safety_buffer_blocks_illegal_llm_commands():
    """Reject 'SELL' commands if SoC is at the safety limit, but permit 'BUY'."""
    sb = SafetyBuffer("test_node", buffer_soc=10.0)
    
    # Sell should be blocked if at buffer
    ok, _ = sb.validate_llm_command({"action": "SELL"}, 10.0)
    assert ok is False
    
    # Buy should be allowed even if at buffer
    ok, _ = sb.validate_llm_command({"action": "BUY"}, 10.0)
    assert ok is True

# ------------------------------------------------------------------
# Failover Manager Tests
# ------------------------------------------------------------------
def test_failover_manager_detects_grid_outage_after_debounce():
    """Verify that voltage drops trigger 'FAILED' status only after a 3-step debounce period."""
    fm = FailoverManager("test_node")
    # VOLTAGE_FAILED_V is 180.0 by default, debounce is 3
    assert fm.assess(230.0) == GridStatus.CONNECTED
    
    # First bad reading
    assert fm.assess(170.0) == GridStatus.CONNECTED # Not triggered yet
    assert fm.assess(170.0) == GridStatus.CONNECTED
    assert fm.assess(170.0) == GridStatus.FAILED    # Triggered on 3rd reading
    
    # Recovery is immediate
    assert fm.assess(230.0) == GridStatus.CONNECTED

# ------------------------------------------------------------------
# MQTT Handshake Request/Response Logic
# ------------------------------------------------------------------
def test_handshake_serialization():
    from orchestrator.mqtt_handshake import HandshakePayload
    import json
    from dataclasses import asdict
    
    p = HandshakePayload("node1", "node2", 1.5, 7.0, "req1", "2024-01-01T00:00:00")
    d = asdict(p)
    assert d["sender_id"] == "node1"
    assert json.loads(json.dumps(d)) == d
