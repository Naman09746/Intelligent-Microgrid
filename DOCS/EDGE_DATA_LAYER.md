# Edge Data Layer — Technical Documentation

## Table of Contents

1. [What is the Edge Data Layer?](#1-what-is-the-edge-data-layer)
2. [Where Does It Fit in the System?](#2-where-does-it-fit-in-the-system)
3. [Files & Code Breakdown](#3-files--code-breakdown)
4. [Does the Edge Layer Train the Model?](#4-does-the-edge-layer-train-the-model)
5. [Flexibility & Scalability](#5-flexibility--scalability)
6. [Industry Standards & Real-World Deployability](#6-industry-standards--real-world-deployability)
7. [Quick Start Commands](#7-quick-start-commands)
8. [What Was Changed from the Teammate's Code](#8-what-was-changed-from-the-teammates-code)

---

## 1. What is the Edge Data Layer?

The Edge Data Layer is the **real-time data backbone** of the microgrid system. Think of it as the "nervous system" that collects live sensor readings from every home in the neighbourhood and makes that data available — privately and securely — to the AI decision-making layers above.

### In Plain Terms

Every home in the microgrid has physical equipment: solar panels, a battery, appliances drawing load, and a connection to the main grid. These devices produce a continuous stream of measurements:

| Measurement | What It Means |
|:---|:---|
| `voltage_v` | Voltage at the home's meter (≈230V in India) |
| `current_a` | Current being drawn by the house |
| `power_solar_kw` | How much the rooftop panels are generating right now |
| `power_load_kw` | How much electricity the house is consuming right now |
| `soc_pct` | Battery State-of-Charge (0%–100%) |
| `battery_power_kw` | Charge (+) or discharge (−) rate of the home battery |
| `grid_import_kw` | Power being bought from the main grid |
| `grid_export_kw` | Power being sold back to the main grid |

The Edge Data Layer's job is to:

1. **Collect** these readings every few seconds via MQTT (a lightweight messaging protocol designed for IoT).
2. **Store** them in a **private SQLite database** — one per home. No home's raw data ever leaves its own database file.
3. **Summarise** the data into safe, anonymised summaries (e.g., "this home has 0.5 kW surplus") that the LLM Agent and Orchestrator can consume.

### The Privacy Guarantee

This is the "Privacy-by-Design" principle from the project architecture:

```
┌─────────────────────────────────────────────────────┐
│                  HOME NODE (delhi_01)               │
│                                                     │
│  Sensors ──► MQTT ──► EdgeMQTTClient ──► SQLite DB  │
│                                      (PRIVATE)      │
│                                          │          │
│                               get_summary()         │
│                                          │          │
│                                  NodeSummary        │
│                            (only this leaves)       │
└──────────────────────────────┬──────────────────────┘
                               │ anonymised intent:
                               │ "SURPLUS 0.5 kW"
                               ▼
                    Tactical Orchestrator / LLM Agent
```

The raw time-series of voltage, current, second-by-second consumption? That stays on-device in `node_delhi_01.db`. An outsider looking at the marketplace would never know what time you turned on your AC or how your electricity consumption looks hour-by-hour. Only the **aggregated surplus/deficit intent** is shared.

---

## 2. Where Does It Fit in the System?

The project has **5 layers** stacked on top of each other:

```
Layer 5 ─── Central Marketplace (PostgreSQL)  ◄── Phase 5
                 │
Layer 4 ─── Strategic LLM Agent               ◄── Phase 4
                 │ reads NodeSummary + forecasts
Layer 3 ─── Tactical Orchestrator (FSM)        ◄── Phase 3
                 │ reads real-time SoC + safety buffer
Layer 2 ─── ★ Edge Data Layer (MQTT + SQLite)  ◄── THIS (Phase 2)
                 │ ingests sensor telemetry
Layer 1 ─── Predictive Forecasting Engine      ◄── DONE (Solar + Load XGBoost)
```

### How the layers interact

| Consumer | What it reads from the Edge Layer | Why |
|:---|:---|:---|
| **Tactical Orchestrator** (Phase 3) | `EdgeNode.get_latest_reading()` → real-time SoC, voltage | To enforce the 10% safety buffer and trigger emergency failover in sub-second time |
| **Strategic LLM Agent** (Phase 4) | `EdgeNode.get_status()` → `NodeSummary` | To reason about surplus/deficit across the neighbourhood and negotiate P2P trades |
| **LLM Agent** (Phase 4) | `EdgeNode.get_history(hours=24)` → DataFrame | To pass recent actual data into the Solar/Load Forecasters for 24-hour predictions |
| **Central Marketplace** (Phase 4) | Posted by the LLM Agent (not the edge layer directly) | Anonymised trade intents ("I have 2 kWh surplus at ₹5/kWh") |

---

## 3. Files & Code Breakdown

All files are in the `edge/` package (9 Python files):

### 3.1 `edge/config.py` — Central Configuration

**Purpose**: Single source of truth for all constants. No hardcoded broker addresses or node names anywhere else.

**Key contents**:

```python
NODE_CONFIGS = {
    "delhi_01":      {"city": "Delhi",      "lat": 28.6139, "lon": 77.2090, "battery_capacity_wh": 10_000},
    "noida_02":      {"city": "Noida",      "lat": 28.5355, "lon": 77.3910, "battery_capacity_wh": 10_000},
    "gurugram_03":   {"city": "Gurugram",   "lat": 28.4595, "lon": 77.0266, "battery_capacity_wh": 10_000},
    "chandigarh_04": {"city": "Chandigarh", "lat": 30.7333, "lon": 76.7794, "battery_capacity_wh": 12_000},
    "dehradun_05":   {"city": "Dehradun",   "lat": 30.3165, "lon": 78.0322, "battery_capacity_wh":  8_000},
}
```

Every setting can be overridden via environment variables (e.g., `MQTT_BROKER=192.168.1.10`), which is the standard pattern for Docker/cloud deployments.

### 3.2 `edge/models.py` — Data Models

**Purpose**: Type-safe data structures used everywhere in the edge layer.

| Class | What it represents | When it's used |
|:---|:---|:---|
| `TelemetryReading` | One sensor snapshot (10 fields) | Published via MQTT, stored in SQLite |
| `NodeSummary` | Aggregated view of a node | Returned by `get_summary()` — the only data that leaves the edge |

Both have `to_json()` / `from_json()` / `to_dict()` / `from_dict()` helpers for clean serialisation. `TelemetryReading.from_dict()` is **backwards-compatible** with the teammate's W-unit payload format (it auto-converts `solar_w` → `power_solar_kw`).

Also defines the SQLite `CREATE TABLE` SQL as constants (not buried in function bodies).

### 3.3 `edge/database.py` — Private SQLite Manager

**Purpose**: One `EdgeDatabase` instance per home node. Manages the private `node_{id}.db` file.

| Method | What it does |
|:---|:---|
| `initialize()` | Creates the `telemetry` table + index if they don't exist |
| `insert_reading(reading)` | Write one `TelemetryReading` to the DB |
| `insert_batch(readings)` | Bulk insert (more efficient for replays) |
| `get_latest(n)` | Most recent N readings |
| `get_range(start, end)` | All readings in a time window |
| `get_summary(hours)` | **Aggregated `NodeSummary`** — avg load, avg solar, net energy, intent (SURPLUS/DEFICIT/BALANCED), current SoC |
| `cleanup(retention_hours)` | Delete readings older than 7 days (prevents unbounded growth) |
| `row_count()` | Health check — how many rows are stored |

Uses **WAL mode** (Write-Ahead Logging) so reads and writes don't block each other — important when the MQTT loop is writing while the Orchestrator is reading.

### 3.4 `edge/mqtt_client.py` — MQTT Subscriber

**Purpose**: Subscribes to `microgrid/{node_id}/telemetry` and writes every incoming message to the node's private database.

Key design decisions:
- **Class-based** (`EdgeMQTTClient`), not module-level functions — easier to test and extend.
- **Single-node subscription** — each instance only listens to its own topic (privacy).
- **Auto-reconnect** with exponential back-off (1s → 120s).
- **QoS 1** — guaranteed delivery (MQTT standard for telemetry).
- Validates that incoming `node_id` matches the subscribed node (rejects foreign data).

### 3.5 `edge/simulator.py` — Telemetry Generator

**Purpose**: Generates realistic fake telemetry for **all 5 nodes simultaneously**. This is how you test the system without real hardware.

The physics models come from your project's own Forecasting Engine:
- **Solar**: Bell-curve generation (6 AM → noon peak → 6 PM), with cloud noise — mirrors `SolarForecaster` patterns.
- **Load**: Double-peak residential curve (morning 7–9 AM, evening 6–9 PM) — mirrors `LoadForecaster` patterns.
- **Battery SoC**: Evolves realistically based on net energy (solar − load) each tick.
- **Grid import/export**: Computed when battery can't absorb the surplus/deficit.

Each node has its own seeded RNG, so runs are reproducible.

### 3.6 `edge/node.py` — EdgeNode Orchestrator

**Purpose**: **The main API class** that Phase 3 and Phase 4 will import. Wires database + MQTT together into a single object.

```python
from edge.node import EdgeNode

node = EdgeNode("delhi_01")
node.start()                          # Init DB + connect MQTT

# Phase 3 (Orchestrator) uses:
reading = node.get_latest_reading()   # Real-time SoC check
status  = node.get_status(hours=1)    # NodeSummary for decisions

# Phase 4 (LLM Agent) uses:
history = node.get_history(hours=24)  # DataFrame for Forecaster input

node.stop()
```

### 3.7 `edge/run_node.py` — CLI Entry Point (Ingestion)

```bash
python -m edge.run_node                  # Start all 5 nodes
python -m edge.run_node --node delhi_01  # Start one node
python -m edge.run_node --broker 10.0.0.5 --port 1883
```

### 3.8 `edge/run_simulator.py` — CLI Entry Point (Simulator)

```bash
python -m edge.run_simulator                           # Default: 5s interval, 1-min step
python -m edge.run_simulator --interval 1 --step 15    # Fast demo: 1s real = 15-min simulated
python -m edge.run_simulator --ticks 96                # Run exactly 24 simulated hours
```

### 3.9 `edge/broker.py` — Development MQTT Broker

A lightweight Python-based MQTT broker using `amqtt` for when you don't have Mosquitto installed. **For development only** — production should use Mosquitto.

```bash
python -m edge.broker    # Starts on localhost:1883
```

---

## 4. Does the Edge Layer Train the Model?

**No. The Edge Layer has nothing to do with model training.**

Here's the clear separation:

| Component | Phase | What it does | Data source |
|:---|:---|:---|:---|
| `forecasting/load/data_curator.py` | Phase 1 | **Generates training data** (5 years × 5 cities × 15 homes) | NASA POWER API (historical) |
| `forecasting/load/forecaster.py` | Phase 1 | **Trains the XGBoost model** | `load_data_north_india.csv` (~3.28M rows) |
| **`edge/` (this layer)** | **Phase 2** | **Runtime data collection** for live inference, not training | **Simulated real-time sensors** |

The relationship:

```
                TRAINING (offline, once)
                ========================
  NASA API data (5 years) ──► XGBoost model trained ──► load_model.json (SAVED)

                RUNTIME (live, ongoing)
                =======================
  Edge Layer (real-time) ──► EdgeNode.get_history(24h) ──► LoadForecaster.predict_24h()
                                                              │
                                                    uses load_model.json (LOADED)
                                                              │
                                                     24-hour demand forecast
                                                              │
                                                     LLM Agent uses this for
                                                     P2P trading decisions
```

The Edge Layer provides the **context window** — the last 24–48 hours of actual measurements — which the already-trained model uses to generate its next 24-hour forecast. It does NOT retrain anything.

Think of it like a weather station vs a weather model. The weather model was trained on decades of historical data. The weather station collects today's observations, which the model uses as input to forecast tomorrow. The Edge Layer is the weather station.

---

## 5. Flexibility & Scalability

### Adding More Nodes

To add a 6th home, you only need to edit **one dictionary** in `edge/config.py`:

```python
NODE_CONFIGS = {
    # ... existing 5 ...
    "jaipur_06": {"city": "Jaipur", "lat": 26.9124, "lon": 75.7873, "battery_capacity_wh": 10_000},
}
```

Everything else — the simulator, run_node, database creation — automatically picks up the new node. No code changes required anywhere else.

### Changing a Home's Load Profile

The simulator's `_simulate_load_kw()` function generates the default double-peak pattern. To customise per-node, you can:

1. Add a `"load_profile"` key to `NODE_CONFIGS` (e.g., `"industrial"`, `"residential_heavy"`)
2. Switch the load function based on that profile

This is a straightforward extension — the architecture supports it because each node is independently configured.

### Scaling to 50+ Nodes

The current design already handles this:
- **SQLite per node**: Each home write-locks only its own file. 50 nodes = 50 independent databases. No contention.
- **MQTT topics**: `microgrid/{node_id}/telemetry` — each node has its own topic. The broker handles routing.
- **Ingestion**: Each `EdgeMQTTClient` runs in its own thread. You can run 50 in one process or split across machines.

The bottleneck at scale would be the MQTT broker (Mosquitto handles ~100K messages/second on commodity hardware, so 50 nodes at 1 msg/sec is trivial).

### Docker Deployment

Every config reads from environment variables:

```yaml
# docker-compose.yml (example)
services:
  node_delhi:
    image: microgrid-edge
    environment:
      HOME_ID: delhi_01
      MQTT_BROKER: mosquitto
      DB_DIR: /data/edge
    volumes:
      - delhi_data:/data/edge
```

---

## 6. Industry Standards & Real-World Deployability

### What follows industry standards ✅

| Aspect | Standard/Pattern | Our Implementation |
|:---|:---|:---|
| **MQTT protocol** | ISO/IEC 20922 (OASIS standard) | Used via `paho-mqtt` — the official Eclipse Paho client used by AWS IoT, Azure IoT, Google Cloud IoT |
| **QoS 1 delivery** | MQTT guaranteed delivery | All telemetry published with QoS 1 (at-least-once) |
| **Topic hierarchy** | IEC 61850 / OpenFMB style | `microgrid/{node_id}/telemetry` — clean namespace per device |
| **Edge-first privacy** | GDPR "Privacy by Design" / India's DPDP Act 2023 | Raw sensor data stays on-device in SQLite. Only aggregated intents are shared. |
| **SQLite for edge storage** | Used by Android, iOS, Firefox, every embedded system | Single-file, zero-admin, ACID-compliant. Perfect for edge nodes. |
| **Environment variable config** | 12-Factor App methodology | Every setting overridable via env vars for containerisation |
| **Message schema** | JSON payloads with typed fields | `TelemetryReading` dataclass ensures schema validation |
| **Data retention policy** | GDPR Article 5(1)(e), energy data regulations | 7-day default retention with automatic cleanup |

### What would need upgrades for real production ⚠️

| Gap | Why | What's needed |
|:---|:---|:---|
| **MQTT authentication** | Our broker has `topic-check: False` | Add TLS certificates + username/password auth (Mosquitto supports this natively) |
| **Encryption at rest** | SQLite files are plain-text | Use SQLCipher (encrypted SQLite) — a drop-in replacement library |
| **Encryption in transit** | MQTT payloads are plain JSON | Enable MQTT over TLS (port 8883) — 2 lines of config change |
| **Real sensors** | We use a software simulator | Replace `edge/simulator.py` with actual Modbus/RS485 sensor readers (the MQTT publishing interface stays identical) |
| **Time synchronisation** | We use system clock | Deploy NTP on all nodes (standard for any distributed system) |
| **Monitoring / alerting** | We log to console | Add Prometheus metrics exporter + Grafana dashboard |

### Is this useful in the real world?

**Yes, absolutely.** This exact architecture pattern (MQTT + edge SQLite + aggregated summaries) is used in production by:

- **Schneider Electric EcoStruxure** — their microgrid controllers use MQTT for inter-device communication
- **ABB Ability™ EDCS** — edge data collection systems for industrial microgrids
- **SolarEdge / Enphase** — residential solar monitoring uses similar hub-and-spoke telemetry
- **OpenEMS** — open-source energy management system (MQTT backbone)

The difference between our project and these systems is that we add **LLM-based strategic reasoning** on top (Phase 4), which is a genuine research contribution. The Edge Data Layer itself is solid industrial engineering.

---

## 7. Quick Start Commands

### Prerequisites
```bash
pip install -r requirements.txt
```

### Full demo (3 terminals)

**Terminal 1 — Start MQTT broker:**
```bash
# Option A: Using our dev broker
python -m edge.broker

# Option B: Using Mosquitto (recommended)
mosquitto -v
```

**Terminal 2 — Start sensor simulator:**
```bash
# Simulates all 5 nodes, 1 tick/second, 15 simulated minutes per tick
python -m edge.run_simulator --interval 1 --step 15
```

**Terminal 3 — Start ingestion nodes:**
```bash
# Start all 5 ingestion nodes
python -m edge.run_node
```

### Verify data was ingested
```python
from edge.node import EdgeNode

node = EdgeNode("delhi_01")
node._db.initialize()

# Check how many readings were stored
print(f"Rows: {node._db.row_count()}")

# Get the anonymised summary
summary = node.get_status(hours=1)
print(f"Intent: {summary.intent}")
print(f"Net energy: {summary.net_energy_kw:.2f} kW")
print(f"SoC: {summary.current_soc_pct:.1f}%")
```

---

## 8. What Was Changed from the Teammate's Code

### Files that were kept (concept reused, code rewritten properly)

| Teammate's file | → Our file | What changed |
|:---|:---|:---|
| `mqtt/config.py` | `edge/config.py` | Expanded from 1 hardcoded `home_101` to 5-node `NODE_CONFIGS` dict. Added topic helper functions, retention hours, DB directory config. |
| `mqtt/sensor_simulator.py` | `edge/simulator.py` | Solar/load math kept. Rewritten as `MicrogridSimulator` class publishing for all 5 nodes. Added battery kinetics, grid import/export, kW units, configurable ticks. |
| `mqtt/ingestion_service.py` | `edge/mqtt_client.py` + `edge/database.py` | Split into two clean classes. DB uses WAL mode, proper schema, `get_summary()` API, data retention. MQTT uses per-node subscription and `TelemetryReading` validation. |
| `mqtt/broker.py` | `edge/broker.py` | Copied as-is (it was fine). Moved into the `edge/` package. |

### Files that were written from scratch (did not exist in teammate's code)

| File | Why it was needed |
|:---|:---|
| `edge/models.py` | Type-safe `TelemetryReading` + `NodeSummary` dataclasses. The teammate used raw dicts everywhere — fragile and untyped. |
| `edge/node.py` | `EdgeNode` API class — the single entry point for Phase 3 and 4. The teammate had no equivalent; upper layers would have had to manage DB connections and MQTT subscriptions themselves. |
| `edge/run_node.py` | Proper CLI with `argparse` — supports `--node`, `--broker`, `--port` flags. The teammate had no equivalent. |
| `edge/run_simulator.py` | Proper CLI with `--interval`, `--step`, `--ticks` flags. The teammate's `run_demo.py` was a copy-paste of the simulator with hardcoded values. |

### Files from teammate that were NOT merged (removed/superseded)

| File | Why excluded |
|:---|:---|
| `mqtt/tactical_orchestrator.py` | This is **Phase 3**, not Phase 2. It should be a separate module that imports from `edge/`, not bundled inside the MQTT package. Also had severe code quality issues (AI-generated filler comments). Will be reimplemented properly in Phase 3. |
| `mqtt/market_summarizer.py` | This is **Phase 4** logic (marketplace integration). Will use `EdgeNode.get_status()` when Phase 4 is built. |
| `run_demo.py` | Duplicate of `sensor_simulator.py`. Replaced by `python -m edge.run_simulator`. |
| `test_orchestrator.py` | Tests for Phase 3 code. Will be rewritten when the Orchestrator is properly implemented. |
| `test_summarizer.py` | Tests for Phase 4 code. Same as above. |
| `verify_db.py` | Ad-hoc script. Replaced by `EdgeDatabase.row_count()` + `get_summary()`. |
| `data/home_101.db` | Binary DB file committed to git. Should never be in version control. |
| `marketplace.db` | Same — binary file that should be gitignored. |
| `mqtt/requirements.txt` | Dependencies were in the wrong location. Merged into the root `requirements.txt`. |

---

*Last updated: 15 March 2026*
