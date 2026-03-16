"""
orchestrator/run_orchestrator.py
===============================
CLI entry point to run the Tactical Orchestrator for a specific node.

Usage:
------
  python -m orchestrator.run_orchestrator --node-id delhi_01
"""
import argparse
import signal
import sys
import time
from edge.node import EdgeNode
from orchestrator.orchestrator import TacticalOrchestrator

def main():
    parser = argparse.ArgumentParser(description="Microgrid Tactical Orchestrator")
    parser.add_argument("--node-id", required=True, help="Node ID (e.g. delhi_01)")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    args = parser.parse_args()

    # 1. Start EdgeNode (manages database and telemetry ingestion)
    node = EdgeNode(args.node_id, broker_host=args.broker, broker_port=args.port)
    if not node.start():
        print(f"Failed to start EdgeNode '{args.node_id}'. Is the broker running?")
        sys.exit(1)

    # 2. Start Tactical Orchestrator (manages safety and logic)
    orch = TacticalOrchestrator(args.node_id, node)
    orch.start(broker_host=args.broker, broker_port=args.port)

    # 3. Graceful shutdown handler
    def shutdown_handler(sig, frame):
        print(f"\n[{args.node_id}] Shutting down orchestrator...")
        orch.stop()
        node.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    print(f"[{args.node_id}] Tactical Orchestrator is ACTIVE. Monitoring safety...")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        shutdown_handler(None, None)

if __name__ == "__main__":
    main()
