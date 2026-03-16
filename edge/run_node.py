"""
edge/run_node.py
================
CLI entry point to start one or all EdgeNodes.

Usage
-----
  # Start all 5 nodes (default)
  python -m edge.run_node

  # Start a single specific node
  python -m edge.run_node --node delhi_01

  # Run with a custom broker
  python -m edge.run_node --broker 192.168.1.10 --port 1883
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time

from edge.config import NODE_CONFIGS, MQTT_BROKER, MQTT_PORT
from edge.node import EdgeNode

logger = logging.getLogger("Edge.RunNode")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start Edge Data Layer node(s) — MQTT → SQLite ingestion pipeline."
    )
    parser.add_argument(
        "--node", "-n",
        default=None,
        choices=list(NODE_CONFIGS.keys()),
        help="Single node id to start. Omit to start all 5 nodes.",
    )
    parser.add_argument("--broker", default=MQTT_BROKER, help="MQTT broker host")
    parser.add_argument("--port",   default=MQTT_PORT,   type=int, help="MQTT broker port")
    return parser.parse_args()


def main() -> None:
    args     = parse_args()
    node_ids = [args.node] if args.node else list(NODE_CONFIGS.keys())

    print("=" * 55)
    print("  Edge Data Layer — MQTT Ingestion Nodes")
    print("=" * 55)
    print(f"  Broker   : {args.broker}:{args.port}")
    print(f"  Nodes    : {', '.join(node_ids)}")
    print("=" * 55)

    nodes: list[EdgeNode] = []
    for nid in node_ids:
        node = EdgeNode(nid, broker_host=args.broker, broker_port=args.port)
        if node.start():
            nodes.append(node)
            print(f"  ✅ {nid:20s} ({node.city})")
        else:
            print(f"  ❌ {nid:20s} — could not connect to broker.")

    if not nodes:
        print("\nNo nodes started. Is the MQTT broker running?")
        sys.exit(1)

    print(f"\nIngesting telemetry for {len(nodes)} node(s). Press Ctrl+C to stop.\n")

    # Graceful shutdown on SIGINT / SIGTERM
    def shutdown(sig, frame):
        print("\nShutting down...")
        for n in nodes:
            n.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Periodic status log so the terminal is not silent
    while True:
        time.sleep(30)
        for n in nodes:
            summary = n.get_status(hours=1)
            if summary:
                logger.info(
                    f"[{n.node_id}] rows={n._db.row_count()} | "
                    f"intent={summary.intent} | SoC={summary.current_soc_pct:.1f}%"
                )


if __name__ == "__main__":
    main()
