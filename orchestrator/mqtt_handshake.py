"""
orchestrator/mqtt_handshake.py
==============================
Handles orchestrator-to-orchestrator handshake protocol via MQTT.
Allows peers to confirm energy availability before a trade is finalized.
"""
import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from typing import Dict, Optional

from edge import config

logger = logging.getLogger("Orchestrator.Handshake")

@dataclass
class HandshakePayload:
    sender_id: str
    target_id: str
    amount_kwh: float
    price_inr: float
    request_id: str
    timestamp: str

class HandshakeResult:
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    TIMEOUT = "TIMEOUT"

class MQTTHandshake:
    """
    Manages P2P handshakes using MQTT request/response topics.
    """
    def __init__(self, node_id: str, mqtt_client):
        self.node_id = node_id
        self._mqtt = mqtt_client
        self._pending_responses: Dict[str, threading.Event] = {}
        self._results: Dict[str, str] = {}

    def initiate(self, target_id: str, amount: float, price: float) -> str:
        """
        Send a handshake request and wait (block) for a response.
        Returns HandshakeResult.
        """
        request_id = f"req_{int(time.time())}_{target_id}"
        payload = HandshakePayload(
            sender_id=self.node_id,
            target_id=target_id,
            amount_kwh=amount,
            price_inr=price,
            request_id=request_id,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%S")
        )
        
        topic = config.handshake_request_topic(target_id)
        event = threading.Event()
        self._pending_responses[request_id] = event
        
        logger.info(f"[{self.node_id}] Initiating handshake with {target_id} | {amount}kWh @ ₹{price}")
        self._mqtt.publish(topic, json.dumps(asdict(payload)), qos=2) # QoS 2 for reliability
        
        # Block for response (max 5 seconds)
        success = event.wait(timeout=5.0)
        
        result = self._results.pop(request_id, HandshakeResult.TIMEOUT) if success else HandshakeResult.TIMEOUT
        self._pending_responses.pop(request_id, None)
        
        logger.info(f"[{self.node_id}] Handshake {request_id} result: {result}")
        return result

    def handle_response(self, payload_dict: dict):
        """
        Called when a JSON response is received on the local response topic.
        """
        req_id = payload_dict.get("request_id")
        status = payload_dict.get("status")
        
        if req_id in self._pending_responses:
            self._results[req_id] = status
            self._pending_responses[req_id].set()
            
    def send_response(self, request_payload: dict, status: str):
        """
        Send a response to a peer's request.
        """
        target_id = request_payload["sender_id"]
        response_topic = config.handshake_response_topic(target_id)
        
        response = {
            "request_id": request_payload["request_id"],
            "status": status,
            "responder_id": self.node_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
        }
        
        logger.info(f"[{self.node_id}] Sending handshake response to {target_id}: {status}")
        self._mqtt.publish(response_topic, json.dumps(response), qos=2)
