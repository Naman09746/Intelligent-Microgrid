# Intelligent Microgrid — System Integration Report

This report explains the end-to-end operation of the **Intelligent Microgrid** system as of the completion of **Phase 3 (Tactical Orchestration)**. It breaks down the four distinct processes currently running in your environment.

---

## 1. The Communication Hub (The Broker)
**Command**: `python -m edge.broker`

### What it's doing:
It starts an **MQTT Broker** (using the `amqtt` library) on your local machine. Think of this as the "Central Post Office." In a real-world deployment, this would be a **Mosquitto** server running on a central server or a cloud instance.

### What the output means:
- **"Broker running on localhost:1883"**: The system is ready to route messages.
- No other logs usually appear here unless there's a connection error. This process is silent but vital; if this stops, the "Sensors" cannot talk to the "Brain."

---

## 2. The Sensors (The Simulator)
**Command**: `python -m edge.run_simulator`

### What it's doing:
It mimics the physical hardware (Smart Meters, Inverters, PV Panels) for **5 different homes** simultaneously. It uses the physics models derived from your forecasting research to generate realistic data.

### Detailed Output Breakdown:
> `[delhi_01] 2026-03-16T00:13:00 | solar=0.00kW  load=0.34kW  SoC=50.1%`

- **Solar (0.00kW)**: It’s night time in the simulation. Solar generation is zero.
- **Load (0.34kW)**: The house is drawing a baseline current (lights, fans, fridge).
- **SoC (50.1%)**: The State of Charge (Battery). Since there is no solar, the battery is depleting to satisfy the 0.34kW load.
- **15-min Intervals**: Every "tick" advances the clock by 15 minutes, mimicking real-world utility billing cycles.

---

## 3. The Librarian (The Edge Node)
**Command**: `python -m edge.run_node`

### What it's doing:
This is the **Persistence Layer**. It subscribes to the messages being sent by the Simulator and saves them into private, local files.

### Detailed Output Breakdown:
- **"Database opened at data\edge\node_delhi_01.db"**: It is ensuring data privacy by keeping Delhi's data in a separate file from Noida's.
- **"MQTT ingestion loop started"**: The 5 internal clients are now listening.
- **Why it matters**: This follows a **Privacy-by-Design** architecture. Granular electricity data (which can reveal when you are home or sleeping) stays local. Only high-level summaries are ever shared with the marketplace.

---

## 4. The Safety Brain (Tactical Orchestrator)
**Command**: `python -m orchestrator.run_orchestrator --node delhi_01`

### What it's doing:
This is the **"Physical Governor."** It is a sub-second controller that sits between the "AI" and the "Battery." It ensures that no matter what the high-level LLM decides, the physical battery never gets damaged.

### Detailed Output Breakdown:
- **"FSM initialised in state: GRID_CONNECTED"**: The Finite State Machine is alive. It knows the system is currently connected to the utility grid.
- **Monitoring Safety**: It calculates the **"Safe Operating Window"** (e.g., "The battery is at 50%, so I can safely sell 4kWh of energy but I MUST keep the last 1kWh as a backup").
- **State Changes**: If you were to forcedly drop the voltage in the simulator, this terminal would instantly log: `ENTERED ISLANDED MODE`.

---

## 5. The Validation (Pytest)
**Command**: `python -m pytest -v tests/test_orchestrator.py`

### What it's doing:
It runs a suite of automated unit tests to prove the system's "Safety logic" works even before you turn on the simulators.

### Human-Readable Test meanings:
| Test Name | What it technically proves |
|:---|:---|
| `test_fsm_initial_state...` | The system starts safely in Grid mode. |
| `test_fsm_handles_grid_failure...` | The "Auto-Islanding" logic works (Safety against blackouts). |
| `test_safety_buffer_correctly...` | If the battery hits **10%**, the system **instantly stops** all exports/discharge. |
| `test_failover_manager...` | It has "Debounce" logic (it won't panic on a 1-second flicker, but will island after a 3-second drop). |

---

## Summary Table

| Process | Role | Industry Equivalent |
|:---|:---|:---|
| **Broker** | Network | AWS IoT / Mosquitto |
| **Simulator** | Hardware | Smart Meters / Inverters |
| **Edge Node** | Data Warehouse | Local Data Logger / Gateway |
| **Orchestrator** | Controller | Microgrid Controller / Governor |

**Current Goal Accomplished**: You have a fully functioning, privacy-safe, simulated microgrid environment where 5 cities are generating data, saving it to isolated databases, and being monitored by a safety orchestrator that follows strict industrial state-machine rules.
