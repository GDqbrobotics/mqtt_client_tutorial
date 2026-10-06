#!/usr/bin/env python3
"""
Vanta XRF MQTT Application
==========================

Example application for the Vanta XRF instrument that integrates the
``mqtt_client`` library (see ``mqtt_client.py`` in the project root) to
expose the instrument state over MQTT.

State machine
-------------
    +----------------+   connect   +----------------+
    |                |------------>|                |
    |  INITIALIZING  |             |     READY      |
    |                |<------------+                |
    +----------------+   <--- save +----------------+
                          |   measurement           |
                          |   file                   |
                          |                          v
                +----------------+             +----------------+
                |                |             |                |
                |  MEASURING     |   measure  |  MEASURE_COMPLETE|
                |                |<----------|                |
                +----------------+  complete +----------------+

    * ``initializing``      - starting up, connecting to the instrument
    * ``ready``            - idle, waiting for a "measure" command
    * ``measuring``        - an XRF acquisition is in progress
    * ``measure_complete`` - acquisition finished, result being saved

Behaviour
---------
* The current state is published to the *status topic* every 2 seconds.
* When a command of the form ``measure <object_name>`` (optionally quoted,
  e.g. ``measure 'sample_01'``) arrives on the *command topic*, the app
  transitions from ``ready`` to ``measuring`` and triggers an acquisition.
* When the acquisition is taken, the app moves to ``measure_complete`` and
  saves the measurement to a file named after the object (e.g.
  ``sample_01.json``), then returns to ``ready``.

Usage
-----
    # 1. start a local broker (or point at your own broker)
    mosquitto -d -p 1883

    # 2. run the application (simulation by default)
    python vanta_mqtt_app.py

    # against a real Vanta instrument (USB / OTG)
    python vanta_mqtt_app.py --device usb

    # or LAN, with an explicit IP
    python vanta_mqtt_app.py --device lan --ip 192.168.7.2

    # point at an existing broker / custom topics
    python vanta_mqtt_app.py --broker 192.168.1.50 --port 1883 \
        --status-topic vanta/status --command-topic commands/vanta

Sending a test command (from another terminal):
    mosquitto_pub -t "commands/vanta" -m "measure 'sample_01'"
"""

import os
import sys
import json
import time
import signal
import shlex
import random
import argparse
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum

# ---------------------------------------------------------------------------
# Make the project root importable so we can reuse the shared MQTT library.
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from mqtt_client import MQTTClient, MQTTConfig, TopicConfig  # noqa: E402


# ---------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------
class State(Enum):
    """The four states of the Vanta XRF application."""

    INITIALIZING = "initializing"
    READY = "ready"
    MEASURING = "measuring"
    MEASURE_COMPLETE = "measure_complete"


# ---------------------------------------------------------------------------
# Measurer abstraction
# ---------------------------------------------------------------------------
class Measurer(ABC):
    """Abstract backend that performs an XRF measurement."""

    @abstractmethod
    def start(self):
        """Prepare the backend (connect / login)."""

    @abstractmethod
    def measure(self, object_name: str):
        """Block until the measurement for ``object_name`` is taken.

        Returns a JSON-serialisable dict describing the measurement.
        """

    @abstractmethod
    def stop(self):
        """Release the backend."""


class SimulatedMeasurer(Measurer):
    """
    A measurer that fakes an acquisition.

    Used as the default so the application runs without any Vanta hardware.
    It mimics a few seconds of acquisition time and returns a small,
    deterministic-looking result payload.
    """

    SAMPLE_ELEMENTS = ("Fe", "Cu", "Zn", "Pb", "As", "Sb", "Hg", "Cd")

    def __init__(self, duration: float = 3.0):
        self._duration = duration
        self._running = False

    def start(self):
        self._running = True

    def measure(self, object_name: str):
        # Simulate the XRF exposure time.
        time.sleep(self._duration)
        elements = {}
        for el in self.SAMPLE_ELEMENTS:
            elements[el] = round(random.uniform(0.0, 5.0), 3)
        return {
            "object": object_name,
            "simulated": True,
            "timestamp": datetime.now().isoformat(),
            "duration_s": self._duration,
            "elements_pct": elements,
        }

    def stop(self):
        self._running = False


class VantaMeasurer(Measurer):
    """
    Measurer backed by the real Vanta instrument (``vanta_api_client.py``).

    The heavy lifting (WebSocket connect, login, start/stop test, heartbeat,
    notification parsing) is delegated to :class:`VantaClient`.  This is a
    best-effort integration: the instrument must be reachable and logged in
    for an acquisition to complete.
    """

    def __init__(self, connection_type: str = "usb", host: str = None,
                 timeout: float = 120.0):
        # Imported lazily so the simulation path has no websocket dependency.
        from vanta_api_client import VantaClient
        self._client = VantaClient(connection_type=connection_type, host=host)
        self._timeout = timeout
        self._result = None

    def start(self):
        host = self._client.discover_device()
        if not host:
            raise RuntimeError("Could not discover the Vanta device")
        if not self._client.connect():
            raise RuntimeError("Could not connect to the Vanta device")
        if not self._client.login():
            raise RuntimeError("Could not log in to the Vanta device")
        self._client.clear_faults()

    def measure(self, object_name: str):
        self._result = None
        if not self._client.start_test():
            raise RuntimeError("Failed to start the XRF test")

        # Poll for the result notification (commandId 403 / id 206).
        deadline = time.time() + self._timeout
        while self._result is None and time.time() < deadline:
            self._client._listen_for_notification()
            self._client.heartbeat()
            time.sleep(0.2)

        if self._result is None:
            # Try to stop the test cleanly before giving up.
            try:
                self._client.stop_test()
            except Exception:
                pass
            raise RuntimeError("No result received before timeout")

        result = self._result
        return {
            "object": object_name,
            "simulated": False,
            "timestamp": datetime.now().isoformat(),
            "result": result,
        }

    def stop(self):
        try:
            if self._client.ws:
                self._client.ws.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
class VantaXRFApp:
    """
    MQTT-integrated controller for the Vanta XRF instrument.

    Publishes the current state to ``status_topic`` every ``status_interval``
    seconds and reacts to ``measure <object_name>`` commands received on
    ``command_topic``.
    """

    def __init__(self,
                 mqtt_config: MQTTConfig,
                 status_topic: str,
                 command_topic: str,
                 measurer: Measurer,
                 measures_dir: str = "measures",
                 status_interval: float = 2.0,
                 complete_hold: float = 0.5):
        self._mqtt_config = mqtt_config
        self._status_topic = status_topic
        self._command_topic = command_topic
        self._measurer = measurer
        self._measures_dir = measures_dir
        self._status_interval = status_interval
        self._complete_hold = complete_hold

        self._client: MQTTClient = None
        self._state = State.INITIALIZING
        self._current_object = None
        self._lock = threading.RLock()
        self._running = False

        self._status_thread = None

        # The shared MQTT library invokes this callback on the network
        # thread for every message it receives.
        self._mqtt_config.on_message = self._on_message

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> bool:
        """Connect to the broker and begin the state machine."""
        self._client = MQTTClient(self._mqtt_config)
        if not self._client.start():
            logging.error("Failed to connect to the MQTT broker")
            return False

        os.makedirs(self._measures_dir, exist_ok=True)
        self._running = True

        # Publish the status periodically.
        self._status_thread = threading.Thread(
            target=self._status_loop, daemon=True, name="status"
        )
        self._status_thread.start()

        # Move out of INITIALIZING in a worker thread so we do not block
        # the (blocking) measurer.start() on the caller.
        threading.Thread(target=self._initialize, daemon=True,
                          name="init").start()
        return True

    def stop(self):
        """Stop the status loop, the MQTT client and the measurer."""
        self._running = False
        if self._client:
            self._client.stop()
        try:
            self._measurer.stop()
        except Exception:
            pass

    def run_forever(self):
        """Block until Ctrl+C (used by ``main``)."""
        try:
            while self._running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass

    # -- state machine -----------------------------------------------------

    def _set_state(self, state: State, object_name=None):
        with self._lock:
            self._state = state
            if object_name is not None or state is not State.READY:
                self._current_object = object_name
            logging.info("State -> %s", state.value)

    def _current_state(self) -> State:
        with self._lock:
            return self._state

    def _initialize(self):
        """INITIALIZING -> READY."""
        try:
            self._measurer.start()
            # Give the 'initializing' state a moment to be observed.
            time.sleep(1.0)
        except Exception as e:
            logging.error("Initialisation failed: %s", e)
            self._set_state(State.READY)
            return
        self._set_state(State.READY)

    def _start_measurement(self, object_name: str):
        """READY -> MEASURING (spawn the acquisition worker)."""
        with self._lock:
            if self._state is not State.READY:
                logging.warning(
                    "Ignoring 'measure %s': not in READY state "
                    "(current=%s)", object_name, self._state.value
                )
                return
            self._state = State.MEASURING
            self._current_object = object_name
        logging.info("Starting measurement of '%s'", object_name)

        thread = threading.Thread(
            target=self._run_measurement, args=(object_name,),
            daemon=True, name="measure"
        )
        thread.start()

    def _run_measurement(self, object_name: str):
        """Perform the acquisition, then MEASURE_COMPLETE -> READY."""
        try:
            # Block until the measurement is taken.
            data = self._measurer.measure(object_name)

            # The measurement is now taken.
            self._set_state(State.MEASURE_COMPLETE, object_name=object_name)
            self._save_measurement(object_name, data)

            # Hold briefly in MEASURE_COMPLETE so the state is observable.
            time.sleep(self._complete_hold)
        except Exception as e:
            logging.error("Measurement failed: %s", e)
        finally:
            self._set_state(State.READY)

    # -- status publishing --------------------------------------------------

    def _status_loop(self):
        while self._running:
            self._publish_status()
            time.sleep(self._status_interval)

    def _publish_status(self):
        with self._lock:
            state = self._state
            obj = self._current_object
        payload = json.dumps({
            "state": state.value,
            "object": obj,
            "timestamp": time.time(),
        })
        # qos=1 so the state is reliably delivered.
        self._client.publish(self._status_topic, payload, qos=1)

    # -- command handling -----------------------------------------------------

    def _on_message(self, client, userdata, msg):
        """MQTT callback: route incoming commands."""
        if msg.topic != self._command_topic:
            return
        payload = msg.payload.decode("utf-8", errors="replace").strip()
        logging.info("Command received on %s: %r", msg.topic, payload)
        self._handle_command(payload)

    def _handle_command(self, payload: str):
        """Parse ``measure <object_name>`` (name may be quoted / contain spaces)."""
        try:
            parts = shlex.split(payload)
        except ValueError:
            logging.warning("Could not parse command: %r", payload)
            return
        if len(parts) >= 2 and parts[0].lower() == "measure":
            object_name = parts[1]
            if object_name:
                self._start_measurement(object_name)
            else:
                logging.warning("Empty object name in command: %r", payload)
        else:
            logging.warning("Unrecognised command: %r", payload)

    # -- persistence --------------------------------------------------------

    def _save_measurement(self, object_name: str, data: dict):
        """Save ``data`` to ``<measures_dir>/<object_name>.json``."""
        safe = self._safe_filename(object_name)
        path = os.path.join(self._measures_dir, safe + ".json")
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            logging.info("Measurement for '%s' saved to %s", object_name, path)
        except Exception as e:
            logging.error("Failed to save measurement for '%s': %s",
                          object_name, e)

    @staticmethod
    def _safe_filename(name: str) -> str:
        bad = '<>:"/\\|?*'
        for c in bad:
            name = name.replace(c, "_")
        return name.strip() or "measure"


# ---------------------------------------------------------------------------
# Config / CLI
# ---------------------------------------------------------------------------
def build_mqtt_config(args) -> MQTTConfig:
    """
    Build the :class:`MQTTConfig` for the application.

    If ``args.config`` points at a YAML file we load it with the shared
    ``load_config`` helper; otherwise we build the config programmatically
    from the CLI arguments.
    """
    if args.config:
        from mqtt_client import load_config
        return load_config(args.config)

    return MQTTConfig(
        broker=args.broker,
        port=args.port,
        client_id="vanta_xrf_app",
        keepalive=60,
        clean_session=True,
        verbose=True,
        topics=[
            TopicConfig(topic="vanta/status", qos=1, retain=True,
                        direction="send"),
            TopicConfig(topic="commands/vanta", qos=1, direction="receive"),
        ],
    )


def build_measurer(args) -> Measurer:
    if args.device == "usb":
        return VantaMeasurer(connection_type="usb", host=args.ip)
    if args.device == "lan":
        return VantaMeasurer(connection_type="lan", host=args.ip)
    # default: simulation
    return SimulatedMeasurer(duration=args.measure_duration)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Vanta XRF MQTT application (state machine + status pub)")
    parser.add_argument("--config", default=None,
                        help="Path to a YAML MQTT config file")
    parser.add_argument("--broker", default="localhost",
                        help="MQTT broker address (default: localhost)")
    parser.add_argument("--port", type=int, default=1883,
                        help="MQTT broker port (default: 1883)")
    parser.add_argument("--status-topic", default="vanta/status",
                        help="Topic used to publish the state")
    parser.add_argument("--command-topic", default="commands/vanta",
                        help="Topic used to receive 'measure <object>' commands")
    parser.add_argument("--measures-dir", default=os.path.join(SCRIPT_DIR,
                        "measures"),
                        help="Directory where measurements are saved")
    parser.add_argument("--device", choices=["sim", "usb", "lan"],
                        default="sim",
                        help="Measurement backend (default: sim)")
    parser.add_argument("--ip", default=None,
                        help="Vanta device IP (for --device lan/usb)")
    parser.add_argument("--measure-duration", type=float, default=3.0,
                        help="Simulated acquisition duration in seconds")
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = parse_args()

    # Graceful shutdown on Ctrl+C.
    def _signal_handler(signum, frame):
        print("\n[*] Shutting down...")
        app.stop()
        signal.default_int_handler(signum, frame)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    mqtt_config = build_mqtt_config(args)
    measurer = build_measurer(args)

    app = VantaXRFApp(
        mqtt_config=mqtt_config,
        status_topic=args.status_topic,
        command_topic=args.command_topic,
        measurer=measurer,
        measures_dir=args.measures_dir,
        status_interval=2.0,
    )

    print("=" * 60)
    print("VANTA XRF MQTT APPLICATION")
    print("=" * 60)
    print(f"  Broker      : {mqtt_config.broker}:{mqtt_config.port}")
    print(f"  Status topic: {args.status_topic}")
    print(f"  Command topic: {args.command_topic}")
    print(f"  Device      : {args.device}")
    print(f"  Measures dir: {args.measures_dir}")
    print("-" * 60)

    if not app.start():
        print("[!] Failed to start the MQTT client")
        return 1

    print("[+] Application running. Send a command to the broker, e.g.:")
    print(f"    mosquitto_pub -t '{args.command_topic}' -m \"measure 'sample_01'\"")
    print("    (or just 'measure sample_01')")
    print("-" * 60)

    app.run_forever()
    app.stop()
    print("[+] Bye!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
