#!/usr/bin/env python3
"""
MQTT Client Library - A simple, configurable MQTT client for Python applications.

This module provides an easy-to-use MQTT client that can be integrated into
your projects with minimal configuration. It supports multiple topics, QoS levels,
persistent connections, and verbose messaging for debugging.

Author: Giuliano Dami
"""

import yaml
import json
import signal
import sys
import time
import threading
import logging
import subprocess
import socket
import shutil
import tempfile
import os
from dataclasses import dataclass, field
from typing import Optional, Callable, List, Dict, Any
from datetime import datetime
import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TopicConfig:
    """Configuration for a single MQTT topic."""
    topic: str
    qos: int = 1
    retain: bool = False
    direction: str = 'both'  # 'send', 'receive', 'both'
    message_prefix: str = ''
    message_suffix: str = ''


@dataclass
class MQTTConfig:
    """Complete MQTT client configuration."""
    # Connection settings
    broker: str = 'localhost'
    port: int = 1883
    client_id: str = 'python_mqtt_client'
    username: Optional[str] = None
    password: Optional[str] = None
    keepalive: int = 60
    clean_session: bool = True

    # Topic configurations
    topics: List[TopicConfig] = field(default_factory=list)

    # Behavioral settings
    verbose: bool = True
    reconnect_delay: int = 5
    max_reconnect_attempts: int = 10

    # Callbacks (optional, for advanced usage)
    on_connect: Optional[Callable] = None
    on_disconnect: Optional[Callable] = None
    on_message: Optional[Callable] = None

    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.broker:
            errors.append("Broker URL is required")
        
        if self.port not in range(1, 65536):
            errors.append(f"Port must be between 1 and 65536, got {self.port}")
        
        if not self.client_id:
            errors.append("Client ID is required")
        
        if not self.topics:
            errors.append("At least one topic must be configured")
        else:
            for i, topic in enumerate(self.topics):
                if not topic.topic:
                    errors.append(f"Topic {i+1} has no topic name")
                if topic.qos not in [0, 1, 2]:
                    errors.append(f"Topic {i+1} has invalid QoS: {topic.qos}")
                if topic.direction not in ['send', 'receive', 'both']:
                    errors.append(f"Topic {i+1} has invalid direction: {topic.direction}")
        
        return errors


class MQTTClient:
    """
    A simple, configurable MQTT client for Python applications.

    This class wraps the Paho MQTT library to provide an easy-to-use interface
    for subscribing to and publishing messages on MQTT brokers.

    Example usage:
        config = MQTTConfig(
            broker='localhost',
            port=1883,
            topics=[
                TopicConfig(topic='sensors/temperature', qos=1, direction='receive'),
                TopicConfig(topic='commands/control', qos=0, direction='send'),
            ]
        )
        client = MQTTClient(config)
        client.start()
    """

    def __init__(self, config: MQTTConfig):
        """Initialize the MQTT client with the given configuration."""
        self.config = config
        self._client: Optional[mqtt.Client] = None
        self._running = False
        self._received_count = 0
        self._sent_count = 0
        self._last_message_time: Optional[float] = None

    def _create_client(self) -> mqtt.Client:
        """Create and configure the underlying Paho MQTT client."""
        client = mqtt.Client(
            client_id=self.config.client_id,
            clean_session=self.config.clean_session,
            protocol=mqtt.MQTTv311
        )

        # Connection callbacks
        def on_connect(client, userdata, flags, rc):
            """Handle connection events."""
            logger.info(f"Connected with result code: {rc}")
            
            # Call user-provided callback if exists
            if self.config.on_connect:
                try:
                    self.config.on_connect(client, userdata, flags, rc)
                except Exception as e:
                    logger.error(f"Error in on_connect callback: {e}")

            # Subscribe to topics after successful connection
            self._subscribe_topics()

        def on_disconnect(client, userdata, rc):
            """Handle disconnection events."""
            logger.warning(f"Disconnected with result code: {rc}")
            
            # Call user-provided callback if exists
            if self.config.on_disconnect:
                try:
                    self.config.on_disconnect(client, userdata, rc)
                except Exception as e:
                    logger.error(f"Error in on_disconnect callback: {e}")

        def on_message(client, userdata, msg):
            """Handle incoming messages."""
            self._last_message_time = time.time()
            message = self._format_message(msg)
            
            if self.config.verbose:
                logger.info(f"RECEIVED [{msg.topic}] {message}")
            
            self._received_count += 1
            
            # Call user-provided callback if exists
            if self.config.on_message:
                try:
                    self.config.on_message(client, userdata, msg)
                except Exception as e:
                    logger.error(f"Error in on_message callback: {e}")

        def on_publish(client, userdata, mid):
            """Handle publish events."""
            pass  # Silent by default

        def on_subscribe(client, userdata, mid, granted_qos_list):
            """Handle subscription events."""
            logger.info(f"Subscribed successfully")

        def on_unsubscribe(client, userdata, mid):
            """Handle unsubscription events."""
            logger.info(f"Unsubscribed")

        # Set callbacks
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.on_publish = on_publish
        client.on_subscribe = on_subscribe
        client.on_unsubscribe = on_unsubscribe

        # Authentication
        if self.config.username:
            client.username_pw_set(self.config.username, self.config.password)

        # TLS configuration (optional)
        self._setup_tls(client)

        return client

    def _setup_tls(self, client: mqtt.Client):
        """Configure TLS/SSL if certificates are provided."""
        # This can be extended for production use
        pass

    def _subscribe_topics(self):
        """Subscribe to all configured topics."""
        logger.info(f"Subscribing to {len(self.config.topics)} topics...")
        
        for topic_config in self.config.topics:
            # Only subscribe if direction allows receiving or is 'both'
            if topic_config.direction in ['receive', 'both']:
                logger.info(f"Subscribing to: {topic_config.topic} (QoS={topic_config.qos})")
                client = self._client
                if client:
                    client.subscribe(
                        topic_config.topic,
                        qos=topic_config.qos
                    )

    def _format_message(self, msg: mqtt.MQTTMessage) -> str:
        """Format a message for display."""
        try:
            payload = msg.payload.decode('utf-8', errors='replace').strip()
        except Exception:
            payload = str(msg.payload)
        
        return f'"{payload}"'

    def _format_topic(self, topic_config: TopicConfig) -> str:
        """Format a topic with prefix/suffix if configured."""
        full_topic = topic_config.topic
        if topic_config.message_prefix:
            full_topic = topic_config.message_prefix + full_topic
        if topic_config.message_suffix:
            full_topic = full_topic + topic_config.message_suffix
        return full_topic

    def _publish_message(self, topic_config: TopicConfig, payload: str):
        """Publish a message to a configured topic."""
        # Only publish if direction allows sending or is 'both'
        if topic_config.direction not in ['send', 'both']:
            return

        formatted_topic = self._format_topic(topic_config)
        logger.info(f"SENDING [{formatted_topic}] {payload}")
        self._sent_count += 1

        client = self._client
        if not client:
            logger.error("Client not initialized. Call start() first.")
            return

        result = client.publish(
            formatted_topic,
            payload,
            qos=topic_config.qos,
            retain=topic_config.retain
        )

        # Wait for publish to complete
        result.wait_for_publish()
        logger.debug(f"Published message mid={result.mid}")

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> bool:
        """
        Publish a message to a topic.

        Args:
            topic: The MQTT topic to publish to
            payload: The message payload to send
            qos: Quality of Service level (0, 1, or 2)
            retain: Whether to retain the message on the broker

        Returns:
            True if the message was published successfully, False otherwise
        """
        client = self._client
        if not client:
            logger.error("Client not initialized. Call start() first.")
            return False

        logger.info(f"SENDING [{topic}] {payload}")
        self._sent_count += 1

        result = client.publish(topic, payload, qos=qos, retain=retain)
        result.wait_for_publish()
        logger.debug(f"Published message mid={result.mid}")
        return True

    def start(self, reconnect: bool = True) -> bool:
        """
        Start the MQTT client and connect to the broker.

        Args:
            reconnect: Whether to automatically reconnect on disconnection

        Returns:
            True if the client started successfully, False otherwise
        """
        if self._running:
            logger.warning("Client is already running")
            return True

        try:
            # Create the client
            self._client = self._create_client()

            # Configure connection options
            self._client.connect_async(
                self.config.broker,
                self.config.port,
                keepalive=self.config.keepalive
            )

            # Set reconnect options
            if reconnect:
                self._client.reconnect_delay_set(
                    min_delay=self.config.reconnect_delay,
                    max_delay=self.config.reconnect_delay * 2
                )

            # Start the network thread
            self._client.loop_start()

            # Wait for connection
            start_time = time.time()
            max_wait = 30  # seconds
            while time.time() - start_time < max_wait:
                if self._client.is_connected():
                    logger.info("Successfully connected to MQTT broker")
                    self._running = True
                    return True
                time.sleep(1)

            logger.error(f"Failed to connect to broker within {max_wait} seconds")
            return False

        except Exception as e:
            logger.error(f"Error starting client: {e}")
            return False

    def stop(self):
        """Stop the MQTT client and disconnect from the broker."""
        if not self._running:
            return

        logger.info("Stopping MQTT client...")
        self._running = False

        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None

    def is_connected(self) -> bool:
        """Check if the client is currently connected to the broker."""
        if not self._client:
            return False
        return self._client.is_connected()

    def get_stats(self) -> Dict[str, Any]:
        """Get connection and message statistics."""
        return {
            'connected': self.is_connected(),
            'received_count': self._received_count,
            'sent_count': self._sent_count,
            'last_message_time': self._last_message_time,
            'uptime_seconds': time.time() - self._start_time if hasattr(self, '_start_time') else 0
        }


def load_config(config_path: str) -> MQTTConfig:
    """
    Load configuration from a YAML file.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Parsed MQTTConfig object

    Raises:
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the YAML is malformed
    """
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)

    # Remove keys that are not part of MQTTConfig dataclass
    valid_keys = {
        'broker', 'port', 'client_id', 'username', 'password',
        'keepalive', 'clean_session', 'verbose', 'reconnect_delay',
        'max_reconnect_attempts'
    }
    
    config_data = {k: v for k, v in data.items() if k in valid_keys}
    config = MQTTConfig(**config_data)

    # Parse topics from config
    topics_data = data.get('topics', [])
    if isinstance(topics_data, list):
        valid_topic_keys = {'topic', 'qos', 'retain', 'direction', 'message_prefix', 'message_suffix'}
        config.topics = [
            TopicConfig(**{k: v for k, v in topic.items() if k in valid_topic_keys})
            for topic in topics_data
        ]

    return config


def _start_mosquitto(port: int = 1883) -> Optional[subprocess.Popen]:
    """
    Start a mosquitto broker as a subprocess.

    Returns the Popen object if successful, None if mosquitto is not available.
    """
    import subprocess
    import shutil
    import tempfile
    import os

def _start_mosquitto(port: int = 1883) -> Optional[subprocess.Popen]:
    """
    Start a mosquitto broker as a subprocess.

    Returns the Popen object if successful, None if mosquitto is not available.
    """
    # Check if mosquitto is available
    mosquitto_path = shutil.which('mosquitto')
    if not mosquitto_path:
        logger.error("mosquitto not found. Please install it: 'sudo apt install mosquitto' or 'brew install mosquitto'")
        return None

    # Create a minimal mosquitto config
    config_fd, config_path = tempfile.mkstemp(suffix='.conf', prefix='mosquitto_')
    with os.fdopen(config_fd, 'w') as f:
        f.write(f"listener {port}\n")
        f.write("allow_anonymous true\n")
        f.write("persistence false\n")
        f.write("log_dest stdout\n")

    # Start mosquitto in the background
    try:
        proc = subprocess.Popen(
            [mosquitto_path, '-c', config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info(f"Mosquitto broker started (PID: {proc.pid})")
        return proc
    except Exception as e:
        logger.error(f"Failed to start mosquitto: {e}")
        return None


def _stop_mosquitto(proc: Optional[subprocess.Popen]) -> None:
    """
    Stop a mosquitto broker subprocess.
    """
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info("Mosquitto broker stopped")


def _wait_for_broker(broker_host: str, port: int, timeout: int = 10) -> bool:
    """Wait for the broker to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.create_connection((broker_host, port), timeout=1)
            sock.close()
            logger.info("Broker is ready")
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


def run_test_with_broker(config: MQTTConfig, interactive: bool = False) -> None:
    """
    Spawn a local mosquitto broker and run the client against it.

    The broker is launched automatically and stopped on exit.
    """
    logger.info("Starting embedded MQTT broker...")
    broker_proc = _start_mosquitto(config.port)

    if not broker_proc:
        logger.error("Could not start broker. Please start mosquitto manually:")
        logger.error("  mosquitto -d -p 1883")
        return

    if not _wait_for_broker(config.broker, config.port):
        logger.error("Broker did not become ready in time")
        _stop_mosquitto(broker_proc)
        return

    # Run the client (with optional interactive mode)
    client = MQTTClient(config)
    if not client.start():
        logger.error("Failed to connect to broker")
        _stop_mosquitto(broker_proc)
        return

    try:
        if interactive:
            _run_interactive(client, config)
        else:
            _run_normal_mode(client)
    finally:
        client.stop()
        _stop_mosquitto(broker_proc)


def _run_interactive(client: MQTTClient, config: MQTTConfig) -> None:
    """
    Interactive REPL where the user can publish messages to topics.

    Commands:
      publish <topic> <message>  - Send a message
      topics                     - List configured topics
      stats                     - Show stats
      help                     - Show help
      quit                     - Exit
    """
    print()
    print("=" * 70)
    print("MQTT Interactive Mode")
    print("=" * 70)
    print(f"Connected to: {config.broker}:{config.port}")
    print(f"Client ID: {config.client_id}")
    print()
    print("Configured topics:")
    for t in config.topics:
        print(f"  - {t.topic} (QoS={t.qos}, {t.direction})")
    print()
    print("Commands:")
    print("  publish <topic> <message>  - Send a message")
    print("  topics                     - List configured topics")
    print("  stats                     - Show stats")
    print("  help                     - Show help")
    print("  quit                     - Exit")
    print()

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line:
            continue

        parts = line.split(maxsplit=1)
        cmd = parts[0].lower()

        if cmd == 'quit' or cmd == 'exit':
            break
        elif cmd == 'help':
            print("Commands:")
            print("  publish <topic> <message>  - Send a message")
            print("  topics                     - List configured topics")
            print("  stats                     - Show stats")
            print("  quit                     - Exit")
        elif cmd == 'topics':
            for t in config.topics:
                print(f"  - {t.topic} (QoS={t.qos}, {t.direction})")
        elif cmd == 'stats':
            stats = client.get_stats()
            print(f"  Connected: {stats['connected']}")
            print(f"  Received: {stats['received_count']}")
            print(f"  Sent: {stats['sent_count']}")
        elif cmd == 'publish':
            # Parse: publish <topic> <message>
            tokens = line.split()
            if len(tokens) < 3:
                print("Usage: publish <topic> <message>")
                continue
            topic = tokens[1]
            message = ' '.join(tokens[2:])
            if client.publish(topic, message):
                print(f"  Published to '{topic}': {message}")
            else:
                print(f"  Failed to publish to '{topic}'")
        else:
            print(f"Unknown command: {cmd}")
            print("Type 'help' for commands")

    print()
    print("Bye!")


def _run_normal_mode(client: MQTTClient) -> None:
    """
    Run the client in normal mode (no interactive REPL).

    Displays stats periodically until shutdown.
    """
    logger.info("Client running. Press Ctrl+C to stop.")

    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not shutdown_event.is_set():
            stats = client.get_stats()
            print(f"\rReceived: {stats['received_count']}, Sent: {stats['sent_count']}", end='', flush=True)
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()
        logger.info("Client stopped")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='MQTT Client - A simple, configurable MQTT client for Python applications.'
    )
    parser.add_argument(
        '--config', '-c',
        default='config.yaml',
        help='Path to the YAML configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--test-with-broker',
        action='store_true',
        help='Spawn a local mosquitto broker and run the client against it'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Enable interactive mode (REPL) to send messages'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--json-output',
        action='store_true',
        help='Output statistics as JSON'
    )

    args = parser.parse_args()

    config = load_config(args.config)

    if args.test_with_broker:
        run_test_with_broker(config, interactive=args.interactive)
    else:
        client = MQTTClient(config)
        if client.start():
            try:
                if args.interactive:
                    _run_interactive(client, config)
                else:
                    _run_normal_mode(client)
            finally:
                client.stop()
