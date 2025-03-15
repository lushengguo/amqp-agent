#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import json
import time
import random
import datetime
import argparse
from typing import Dict, Any, List, Optional
import threading

# Default configuration
DEFAULT_HOST = "127.0.0.1"  # Rust service address
DEFAULT_PORT = 8080         # Rust service port
DEFAULT_INTERVAL = 2.0      # Send interval (seconds)
DEFAULT_COUNT = -1          # Number of messages to send, -1 means infinite

# RabbitMQ configuration
EXCHANGES = ["test"]
EXCHANGE_TYPES = ["direct"]
ROUTING_KEYS = [""]

class MessageGenerator:
    """Class for generating test messages"""
    def __init__(self):
        self.message_id = 0

    def generate_message(self) -> Dict[str, Any]:
        """Generate a test message"""
        self.message_id += 1

        # Randomly select exchange and routing key
        exchange = random.choice(EXCHANGES)
        exchange_type = EXCHANGE_TYPES[EXCHANGES.index(exchange)]
        routing_key = random.choice(ROUTING_KEYS)

        # Get current timestamp
        timestamp = int(time.time())

        # Generate message content
        message = {
            "url": "amqp://guest:guest@localhost:5672/",
            "exchange": exchange,
            "exchange_type": exchange_type,
            "routing_key": routing_key,
            "message": f"Test message #{self.message_id}, sent at: {datetime.datetime.now().isoformat()}",
            "timestamp": timestamp
        }

        # Construct complete message
        return message

class MessageSender:
    """TCP client for sending messages"""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        """Connect to server"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Failed to connect: {e}")
            return False

    def send_message(self, message: Dict[str, Any]) -> bool:
        if not self.sock:
            if not self.connect():
                return False

        try:
            message_json = json.dumps(message)
            message_bytes = (message_json + "\n").encode("utf-8")
            self.sock.sendall(message_bytes)
            return True
        except Exception as e:
            print(f"Failed to send message: {e}")
            self.sock = None
            return False

def main():
    parser = argparse.ArgumentParser(description="Test data sender for AMQP agent")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Server host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="Send interval in seconds")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of messages to send (-1 for infinite)")
    args = parser.parse_args()

    generator = MessageGenerator()
    sender = MessageSender(args.host, args.port)

    sent_count = 0
    try:
        while args.count == -1 or sent_count < args.count:
            message = generator.generate_message()
            if sender.send_message(message):
                sent_count += 1
                print(f"Sent message {sent_count}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        if sender.sock:
            sender.sock.close()
        print(f"Total messages sent: {sent_count}")

if __name__ == "__main__":
    main() 