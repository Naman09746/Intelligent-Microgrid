"""
edge/config.py
==============
Central configuration for the Edge Data Layer.
All values can be overridden via environment variables for containerised deployments.
No secrets or arbitrary hardcoded values anywhere else in the package.
"""
import os
import logging

# ---------------------------------------------------------------------------
# Multi-node configuration — one entry per physical home in the microgrid.
# Matches the 5 cities used by the Solar and Load Forecasters.
# ---------------------------------------------------------------------------
NODE_CONFIGS = {
    "delhi_01":      {"city": "Delhi",      "lat": 28.6139, "lon": 77.2090, "battery_capacity_wh": 10_000},
    "noida_02":      {"city": "Noida",      "lat": 28.5355, "lon": 77.3910, "battery_capacity_wh": 10_000},
    "gurugram_03":   {"city": "Gurugram",   "lat": 28.4595, "lon": 77.0266, "battery_capacity_wh": 10_000},
    "chandigarh_04": {"city": "Chandigarh", "lat": 30.7333, "lon": 76.7794, "battery_capacity_wh": 12_000},
    "dehradun_05":   {"city": "Dehradun",   "lat": 30.3165, "lon": 78.0322, "battery_capacity_wh":  8_000},
}

# ---------------------------------------------------------------------------
# MQTT Broker — override via env vars for Docker / cloud deployments
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))

# Active node id for single-node processes (simulator / orchestrator).
# When running all nodes, each subprocess sets its own HOME_ID env var.
HOME_ID = os.getenv("HOME_ID", "delhi_01")

# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------
# Mandatory 10 % SoC reserve (N-1 resiliency, matches project spec)
SAFETY_BUFFER_SOC = float(os.getenv("SAFETY_BUFFER_SOC", "10.0"))

# Orchestrator safety thresholds
VOLTAGE_UNSTABLE_V  = float(os.getenv("VOLTAGE_UNSTABLE_V", "200.0"))
VOLTAGE_FAILED_V    = float(os.getenv("VOLTAGE_FAILED_V", "180.0"))
GRID_FAILURE_DEBOUNCE = int(os.getenv("GRID_FAILURE_DEBOUNCE", "3"))

# ---------------------------------------------------------------------------
# Storage — each node writes to its own isolated SQLite file
# ---------------------------------------------------------------------------
DB_DIR                = os.getenv("DB_DIR", os.path.join("data", "edge"))
DATA_RETENTION_HOURS  = int(os.getenv("DATA_RETENTION_HOURS", "168"))   # 7 days

# ---------------------------------------------------------------------------
# MQTT topic map  (teammate's topic design was correct — reused here)
# ---------------------------------------------------------------------------
def telemetry_topic(node_id: str) -> str:
    return f"microgrid/{node_id}/telemetry"

def llm_commands_topic(node_id: str) -> str:
    return f"microgrid/{node_id}/llm_commands"

def safe_window_topic(node_id: str) -> str:
    return f"microgrid/{node_id}/safe_window"

def handshake_request_topic(node_id: str) -> str:
    return f"microgrid/{node_id}/handshake/request"

def handshake_response_topic(node_id: str) -> str:
    return f"microgrid/{node_id}/handshake/response"

# Wildcard topics (used by ingestion services that listen to all nodes)
TOPIC_ALL_TELEMETRY        = "microgrid/+/telemetry"
TOPIC_HANDSHAKE_RESPONSE_ALL = "microgrid/+/handshake/response"
TOPIC_MARKET_SUMMARY       = "microgrid/market/summary"
TOPIC_MARKETPLACE_SETTLE   = "microgrid/marketplace/settle"

# Convenience shortcuts for single-node processes (reads HOME_ID at import time)
TOPIC_TELEMETRY            = telemetry_topic(HOME_ID)
TOPIC_LLM_COMMANDS         = llm_commands_topic(HOME_ID)
TOPIC_SAFE_WINDOW          = safe_window_topic(HOME_ID)
TOPIC_HANDSHAKE_REQUEST    = handshake_request_topic(HOME_ID)

# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------
TELEMETRY_INTERVAL = int(os.getenv("TELEMETRY_INTERVAL", "5"))   # seconds per publish tick

# ---------------------------------------------------------------------------
# Logging  (consistent format across all edge modules)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger("Edge")
