"""
orchestrator/fsm.py
===================
Finite State Machine for the Tactical Orchestrator.
Uses the 'transitions' library to manage microgrid states.
"""
import logging
from transitions import Machine

logger = logging.getLogger("Orchestrator.FSM")

class MicrogridFSM:
    """
    Manages the operational state of a single home node.
    
    States:
    -------
    GRID_CONNECTED : Default state, using utility grid as backup/sink.
    P2P_TRADING    : Active Peer-to-Peer trade in progress.
    ISLANDED       : Disconnected from grid (outage), running on battery+solar.
    EMERGENCY      : Critical state (low battery, grid failure, or hardware fault).
    """
    
    STATES = ["GRID_CONNECTED", "P2P_TRADING", "ISLANDED", "EMERGENCY"]
    
    TRANSITIONS = [
        # Normal operations
        {"trigger": "start_trade",  "source": "GRID_CONNECTED", "dest": "P2P_TRADING"},
        {"trigger": "finish_trade", "source": "P2P_TRADING",    "dest": "GRID_CONNECTED"},
        
        # Grid failure detections
        {"trigger": "grid_failed",  "source": ["GRID_CONNECTED", "P2P_TRADING"], "dest": "ISLANDED"},
        {"trigger": "grid_restored","source": ["ISLANDED", "EMERGENCY"],         "dest": "GRID_CONNECTED"},
        
        # Emergency transitions
        {"trigger": "critical_soc", "source": "*",               "dest": "EMERGENCY"},
        {"trigger": "recover",      "source": "EMERGENCY",       "dest": "ISLANDED", "conditions": "is_grid_absent"},
        {"trigger": "recover",      "source": "EMERGENCY",       "dest": "GRID_CONNECTED", "conditions": "is_grid_present"},
        
        # Islanded trade
        {"trigger": "start_trade",  "source": "ISLANDED",       "dest": "P2P_TRADING"},
        {"trigger": "finish_trade", "source": "P2P_TRADING",    "dest": "ISLANDED", "conditions": "is_grid_absent"},
    ]

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.grid_available = True
        
        self.machine = Machine(
            model=self,
            states=MicrogridFSM.STATES,
            transitions=MicrogridFSM.TRANSITIONS,
            initial="GRID_CONNECTED",
            send_event=True,
            after_state_change="log_transition"
        )
        logger.info(f"[{self.node_id}] FSM initialised in state: {self.state}")

    # ------------------------------------------------------------------
    # Guards & Conditions
    # ------------------------------------------------------------------
    def is_grid_present(self, event):
        return self.grid_available

    def is_grid_absent(self, event):
        return not self.grid_available

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def log_transition(self, event):
        logger.info(f"[{self.node_id}] State transition: {event.transition.source} -> {event.transition.dest}")

    def on_enter_EMERGENCY(self, event):
        logger.warning(f"[{self.node_id}] ENTERED EMERGENCY STATE! Enforcing maximum load shedding.")

    def on_enter_ISLANDED(self, event):
        logger.info(f"[{self.node_id}] ENTERED ISLANDED MODE. Operating on local renewables and battery.")

    def on_enter_P2P_TRADING(self, event):
        logger.info(f"[{self.node_id}] ENTERED P2P_TRADING MODE.")
