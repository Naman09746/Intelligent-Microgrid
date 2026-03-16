"""
edge/run_simulator.py
=====================
CLI entry point to start the multi-node telemetry simulator.

Usage
-----
  # Simulate all 5 nodes at default 5 s / 1 min-step
  python -m edge.run_simulator

  # Faster demo: 1 second real-time, 15 min simulated step
  python -m edge.run_simulator --interval 1 --step 15

  # Run exactly 96 ticks (= 24 simulated hours at 15-min steps)
  python -m edge.run_simulator --interval 1 --step 15 --ticks 96

  # Custom broker
  python -m edge.run_simulator --broker 192.168.1.10
"""
from __future__ import annotations

import argparse

from edge.config import MQTT_BROKER, MQTT_PORT, TELEMETRY_INTERVAL
from edge.simulator import MicrogridSimulator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Microgrid telemetry simulator — publishes synthetic readings for all 5 nodes."
    )
    parser.add_argument("--broker",   default=MQTT_BROKER,       help="MQTT broker host")
    parser.add_argument("--port",     default=MQTT_PORT,          type=int, help="MQTT broker port")
    parser.add_argument("--interval", default=TELEMETRY_INTERVAL, type=int, help="Seconds between publish ticks")
    parser.add_argument("--step",     default=1,                  type=int, help="Simulated minutes per tick")
    parser.add_argument("--ticks",    default=None,               type=int, help="Stop after N ticks (default: run forever)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 60)
    print("  Microgrid Sensor Simulator — All 5 Nodes")
    print("=" * 60)
    print(f"  Broker   : {args.broker}:{args.port}")
    print(f"  Interval : {args.interval}s real-time per tick")
    print(f"  Step     : {args.step} simulated min per tick")
    if args.ticks:
        total_min = args.ticks * args.step
        print(f"  Ticks    : {args.ticks}  (~{total_min // 60}h {total_min % 60}m simulated)")
    else:
        print(f"  Ticks    : ∞  (Ctrl+C to stop)")
    print("=" * 60 + "\n")

    sim = MicrogridSimulator(
        broker_host   = args.broker,
        broker_port   = args.port,
        interval      = args.interval,
        time_step_min = args.step,
    )
    sim.run(ticks=args.ticks)


if __name__ == "__main__":
    main()
