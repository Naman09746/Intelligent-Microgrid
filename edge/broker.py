"""
edge/broker.py
==============
Lightweight in-process MQTT broker using amqtt for local development.
Runs on 0.0.0.0:1883.

Usage
-----
  python -m edge.broker

In production, use Mosquitto instead:
  mosquitto -c mosquitto.conf

Note: amqtt is only needed for local dev. Add it to requirements-dev.txt.
The production broker (Mosquitto) does not require this file.
"""
import asyncio
import logging

from amqtt.broker import Broker

logging.basicConfig(level=logging.INFO)

BROKER_CONFIG = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "0.0.0.0:1883",
        }
    },
    "sys_interval": 10,
    "topic-check": {
        "enabled": False   # Open broker — fine for dev; lock down in production
    },
}


async def run_broker():
    broker = Broker(BROKER_CONFIG)
    await broker.start()
    print("=" * 50)
    print("  MQTT Broker running on localhost:1883")
    print("  (Ctrl+C to stop)")
    print("=" * 50)
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(run_broker())
