#!/usr/bin/env python3
"""
VimbaX Camera MQTT Application
==========================

Example application for the VimbaX Camera instrument that integrates the
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
                          |   file                  |
                          |                         v
                +----------------+           +----------------+
                |                |           |                |
                |  MEASURING     |  measure  |  MEASURE_COMPLETE|
                |                |<----------|                |
                +----------------+  complete +----------------+

    * ``initializing``      - starting up, connecting to the instrument
    * ``ready``            - idle, waiting for a "measure" command
    * ``measuring``        - a camera acquisition is in progress
    * ``measure_complete`` - acquisition finished, result being saved
    * ``error``            - camera initialization failed

Behaviour
---------
* The current state is published to the *status topic* every 2 seconds.
* When a command of the form ``measure <object_name>`` (optionally quoted,
  e.g. ``measure 'sample_01'``) arrives on the *command topic*, the app
  transitions from ``ready`` to ``measuring`` and triggers an acquisition.
* When the acquisition is taken, the app moves to ``measure_complete`` and
  saves a JPEG frame and JSON metadata under a directory named after the
  object, then returns to ``ready``. Commands may also include the camera
  position: ``acquire: sample_01; camera position: [0, 0, 0, 0, 0, 0]``.

Usage
-----
    # 1. start a local broker (or point at your own broker)
    mosquitto -d -p 1883

    # 2. run the application (simulation by default)
    python vimbax_mqtt_app.py

        # against a real VimbaX instrument
    python vimbax_mqtt_app.py --device real

VmbPy must be installed with the Vimba X SDK for real-camera mode.

    # point at an existing broker / custom topics
    python vimbax_mqtt_app.py --broker 192.168.1.50 --port 1883 \
        --status-topic vimbax/status --command-topic commands/vimbax

Sending a test command (from another terminal):
    mosquitto_pub -t "commands/vimbax" -m "acquire: sample_01; camera position: [0, 0, 0, 0, 0, 0]"
"""

import os
import sys
import json
import time
import signal
import shlex
import math
import re
import argparse
import logging
import threading
from contextlib import ExitStack
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
    """States of the VimbaX Camera application."""

    INITIALIZING = "initializing"
    READY = "ready"
    MEASURING = "measuring"
    MEASURE_COMPLETE = "measure_complete"
    ERROR = "error"


# ---------------------------------------------------------------------------
# Measurer abstraction
# ---------------------------------------------------------------------------
class Measurer(ABC):
    """Abstract backend that performs a camera acquisition."""

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

    Used as the default so the application runs without any VimbaX hardware.
    It mimics a few seconds of acquisition time and returns a small,
    deterministic-looking result payload.
    """

    def __init__(self, duration: float = 3.0):
        if not math.isfinite(duration) or duration < 0:
            raise ValueError("Simulation duration must be a finite non-negative value")
        self._duration = duration
        self._running = False

    def start(self):
        self._running = True

    def measure(self, object_name: str):
        # Simulate the Camera exposure time.
        time.sleep(self._duration)
        import numpy as np

        return {
            "image": np.full((64, 64, 3), 127, dtype=np.uint8),
            "object": object_name,
            "simulated": True,
            "timestamp": datetime.now().isoformat(),
            "duration_s": self._duration,
        }

    def stop(self):
        self._running = False


class VimbaXSystem(Measurer):
    """
    VimbaX camera backend
    """

    def __init__(self, camera_id: str = None, timeout: float = 3.0):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Camera timeout must be a finite positive value")
        self._camera_id = camera_id
        self._timeout = timeout
        self._stack = None
        self._camera = None

    def start(self):
        import importlib

        vmbpy = importlib.import_module("vmbpy")

        stack = ExitStack()
        try:
            vmb = stack.enter_context(vmbpy.VmbSystem.get_instance())
            if self._camera_id:
                camera = vmb.get_camera_by_id(self._camera_id)
            else:
                cameras = vmb.get_all_cameras()
                if not cameras:
                    raise RuntimeError("No VimbaX cameras are accessible")
                camera = cameras[0]

            stack.enter_context(camera)
            self._camera = camera
            self._stack = stack

            streams = camera.get_streams()
            if streams:
                try:
                    packet_size = streams[0].GVSPAdjustPacketSize
                    packet_size.run()
                    while not packet_size.is_done():
                        time.sleep(0.01)
                except (AttributeError, vmbpy.VmbFeatureError):
                    logging.debug("Packet-size adjustment is unavailable")
        except BaseException:
            stack.close()
            self._camera = None
            self._stack = None
            raise

    def measure(self, object_name: str):
        if self._camera is None:
            raise RuntimeError("VimbaX camera is not started")

        import importlib

        pixel_format = importlib.import_module("vmbpy").PixelFormat

        for frame in self._camera.get_frame_generator(
                limit=1, timeout_ms=int(self._timeout * 1000)):
            image_frame = frame.convert_pixel_format(pixel_format.Bgr8)
            image = image_frame.as_opencv_image().copy()
            return {
                "image": image,
                "object": object_name,
                "camera_id": self._camera.get_id(),
                "simulated": False,
                "timestamp": datetime.now().isoformat(),
            }
        raise RuntimeError("Camera returned no frame")
    
    def stop(self):
        if self._stack is not None:
            self._stack.close()
        self._camera = None
        self._stack = None

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
class VimbaXCameraApp:
    """
    MQTT-integrated controller for the VimbaX Camera instrument.

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

        self._client = None
        self._state = State.INITIALIZING
        self._current_object = None
        self._lock = threading.RLock()
        self._running = False
        self._stop_event = threading.Event()

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

        try:
            os.makedirs(self._measures_dir, exist_ok=True)
        except OSError:
            self._client.stop()
            self._client = None
            raise
        self._stop_event.clear()
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
        self._stop_event.set()
        if self._client:
            self._client.stop()
        try:
            self._measurer.stop()
        except Exception as e:
            logging.error("Failed to stop camera backend: %s", e)

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
            if self._stop_event.wait(1.0):
                return
        except Exception as e:
            logging.error("Initialisation failed: %s", e)
            self._set_state(State.ERROR)
            return
        self._set_state(State.READY)

    def _start_measurement(self, object_name: str, position=None):
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
            target=self._run_measurement, args=(object_name, position),
            daemon=True, name="measure"
        )
        thread.start()

    def _run_measurement(self, object_name: str, position=None):
        """Perform the acquisition, then MEASURE_COMPLETE -> READY."""
        try:
            acquisition_data = self._measurer.measure(object_name)
            self._set_state(State.MEASURE_COMPLETE, object_name=object_name)
            self._save_measurement(object_name, position, acquisition_data)
            if self._stop_event.wait(self._complete_hold):
                return
        except Exception as e:
            logging.error("Measurement failed: %s", e)
        finally:
            self._set_state(State.READY)

    # -- status publishing --------------------------------------------------

    def _status_loop(self):
        while self._running and not self._stop_event.is_set():
            self._publish_status()
            self._stop_event.wait(self._status_interval)

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
        if self._client is not None:
            self._client.publish(self._status_topic, payload, qos=1,
                                 retain=True)

    # -- command handling -----------------------------------------------------

    def _on_message(self, _client, _userdata, msg):
        """MQTT callback: route incoming commands."""
        if msg.topic != self._command_topic:
            return
        payload = msg.payload.decode("utf-8", errors="replace").strip()
        logging.info("Command received on %s: %r", msg.topic, payload)
        self._handle_command(payload)

    def _handle_command(self, payload: str):
        """Parse measure/acquire commands and start one camera grab."""
        position = None
        if payload.lower().startswith("acquire:"):
            command, separator, position_command = payload.partition(";")
            object_name = self._parse_object_name(command[len("acquire:"):])
            if not object_name:
                logging.warning("Empty object name in command: %r", payload)
                return
            if separator:
                match = re.fullmatch(
                    r"\s*camera\s+position\s*:\s*\[([^\]]*)\]\s*",
                    position_command,
                    flags=re.IGNORECASE,
                )
                if not match:
                    logging.warning("Invalid camera position in command: %r",
                                    payload)
                    return
                try:
                    values = [float(value.strip())
                              for value in match.group(1).split(",")]
                    if len(values) != 6 or not all(map(math.isfinite, values)):
                        raise ValueError
                except ValueError:
                    logging.warning(
                        "Camera position must contain six finite numbers: %r",
                        payload,
                    )
                    return
                position = values
        else:
            try:
                parts = shlex.split(payload)
            except ValueError:
                logging.warning("Could not parse command: %r", payload)
                return
            if not parts or parts[0].lower() != "measure" or len(parts) < 2:
                logging.warning("Unrecognised command: %r", payload)
                return
            object_name = " ".join(parts[1:]).strip()

        self._start_measurement(object_name, position)

    @staticmethod
    def _parse_object_name(value: str) -> str:
        try:
            return " ".join(shlex.split(value.strip())).strip()
        except ValueError:
            return ""

    # -- persistence --------------------------------------------------------

    def _save_measurement(self, object_name: str, position, acquisition_data):
        import cv2

        safe_name = self._safe_filename(object_name)
        output_dir = os.path.join(self._measures_dir, safe_name)
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S_%f")
        basename = "{}_{}".format(safe_name, timestamp)
        image_path = os.path.join(output_dir, basename + ".jpg")
        metadata_path = os.path.join(output_dir, basename + ".json")

        image = acquisition_data.get("image")
        if image is None:
            raise ValueError("Camera acquisition did not contain an image")
        if not cv2.imwrite(image_path, image):
            raise OSError("Failed to write camera image to {}".format(image_path))

        metadata = {
            key: value for key, value in acquisition_data.items()
            if key != "image"
        }
        metadata.update({
            "object": object_name,
            "position": position,
            "image_file": os.path.basename(image_path),
        })
        with open(metadata_path, "w", encoding="utf-8") as metadata_file:
            json.dump(metadata, metadata_file, indent=2)
            metadata_file.write("\n")
        logging.info("Saved camera measurement to %s and %s",
                     image_path, metadata_path)

    @staticmethod
    def _safe_filename(name: str) -> str:
        safe = re.sub(r'[\x00-\x1f<>:"/\\|?*]', "_", name).strip(" .")
        return (safe[:128] or "measure")
        


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

    status_topic = args.status_topic or "vimbax/status"
    command_topic = args.command_topic or "commands/vimbax"
    return MQTTConfig(
        broker=args.broker,
        port=args.port,
        client_id="vimbax_cam_app",
        keepalive=60,
        clean_session=True,
        verbose=True,
        topics=[
            TopicConfig(topic=status_topic, qos=1, retain=True,
                        direction="send"),
            TopicConfig(topic=command_topic, qos=1, direction="receive"),
        ],
    )


def resolve_topics(args, mqtt_config: MQTTConfig):
    """Resolve app topics from CLI overrides or the loaded MQTT config."""
    send_topics = [topic.topic for topic in mqtt_config.topics
                   if topic.direction in ("send", "both")]
    receive_topics = [topic.topic for topic in mqtt_config.topics
                      if topic.direction in ("receive", "both")]
    status_topic = (args.status_topic or
                    (send_topics[0] if args.config and send_topics
                     else "vimbax/status"))
    command_topic = (args.command_topic or
                     (receive_topics[0] if args.config and receive_topics
                      else "commands/vimbax"))

    for topic in mqtt_config.topics:
        if topic.topic == command_topic:
            if topic.direction == "send":
                topic.direction = "both"
            break
    else:
        mqtt_config.topics.append(
            TopicConfig(topic=command_topic, qos=1, direction="receive")
        )
    return status_topic, command_topic


def build_measurer(args) -> Measurer:
    if args.device == "sim":
        return SimulatedMeasurer(duration=args.measure_duration)
    # The real backend is selected explicitly with --device real.
    return VimbaXSystem(camera_id=args.camera_id, timeout=args.timeout)


def parse_args():
    parser = argparse.ArgumentParser(
        description="VimbaX Camera MQTT application (state machine + status pub)")
    parser.add_argument("--config", default=None,
                        help="Path to a YAML MQTT config file")
    parser.add_argument("--broker", default="localhost",
                        help="MQTT broker address (default: localhost)")
    parser.add_argument("--port", type=int, default=1883,
                        help="MQTT broker port (default: 1883)")
    parser.add_argument("--status-topic", default=None,
                        help="Topic used to publish the state")
    parser.add_argument("--command-topic", default=None,
                        help="Topic used to receive 'measure <object>' commands")
    parser.add_argument("--measures-dir", default=os.path.join(SCRIPT_DIR,
                        "measures"),
                        help="Directory where measurements are saved")
    parser.add_argument("--device", choices=["sim", "real"],
                        default="sim",
                        help="Measurement backend (default: sim)")
    parser.add_argument("--measure-duration", type=float, default=3.0,
                        help="Simulated acquisition duration in seconds")
    parser.add_argument("--camera-id", default=None,
                        help="VimbaX camera ID (default: first accessible camera)")
    parser.add_argument("--timeout", type=float, default=3.0,
                        help="Camera frame timeout in seconds")
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
    status_topic, command_topic = resolve_topics(args, mqtt_config)
    measurer = build_measurer(args)

    app = VimbaXCameraApp(
        mqtt_config=mqtt_config,
        status_topic=status_topic,
        command_topic=command_topic,
        measurer=measurer,
        measures_dir=args.measures_dir,
        status_interval=2.0,
    )

    print("=" * 60)
    print("VIMBAX CAMERA MQTT APPLICATION")
    print("=" * 60)
    print(f"  Broker      : {mqtt_config.broker}:{mqtt_config.port}")
    print(f"  Status topic: {status_topic}")
    print(f"  Command topic: {command_topic}")
    print(f"  Device      : {args.device}")
    print(f"  Measures dir: {args.measures_dir}")
    print("-" * 60)

    if not app.start():
        print("[!] Failed to start the MQTT client")
        return 1

    print("[+] Application running. Send a command to the broker, e.g.:")
    print(f"    mosquitto_pub -t '{command_topic}' -m \"measure 'sample_01'\"")
    print("    (or just 'measure sample_01')")
    print("-" * 60)

    app.run_forever()
    app.stop()
    print("[+] Bye!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
