# Vanta XRF — MQTT Application

An example application that controls a **Vanta XRF** instrument over **MQTT**.
It reuses the shared [`mqtt_client`](../../mqtt_client.py) library and wraps it
with a small, explicit **state machine** that is fully driven by MQTT messages.

The application publishes its current state every 2 seconds and reacts to
`measure <object_name>` commands received on a command topic. When a
measurement is taken, the result is saved to a file named after the object.

---

## Table of Contents

- [How it works](#how-it-works)
- [State machine](#state-machine)
- [MQTT topics & messages](#mqtt-topics--messages)
- [Dependencies](#dependencies)
- [Quick start (simulation, no hardware)](#quick-start-simulation-no-hardware)
- [Running against a real Vanta](#running-against-a-real-vanta)
- [CLI options](#cli-options)
- [Saved measurements](#saved-measurements)
- [Testing / monitoring](#testing--monitoring)
- [Project layout](#project-layout)
- [Notes on the real-device backend](#notes-on-the-real-device-backend)

---

## How it works

The app is a thin MQTT front-end for the Vanta instrument:

1. **Publish status** — every **2 seconds** the current state is published to
   the *status topic* as a small JSON object.
2. **Receive commands** — it subscribes to a *command topic*. A message of the
   form `measure <object_name>` (the name may be quoted and may contain spaces)
   starts an acquisition.
3. **Run the measurement** — the measurement is delegated to a *measurer*
   backend (a simulator by default, or the real Vanta API client).
4. **Save the result** — when the acquisition completes the result is written
   to `measures/<object_name>.json` and the app returns to `ready`.

Because the MQTT layer comes from the shared library, this application only
contains the state machine, the command routing and the measurer abstraction —
it is a good reference for how to integrate the `mqtt_client` into a concrete
device.

---

## State machine

```
    +----------------+   connect   +----------------+
    |                |------------>|                |
    |  INITIALIZING  |             |     READY      |
    |                |<------------+                |
    +----------------+   <--- save +----------------+
                          |   measurement           |
                          |   file                  |
                          v                          |
                +----------------+  measure complete  |
                |                |<------------------+
                |  MEASURING     |
                +----------------+
```

| State              | Meaning                                                    |
|--------------------|------------------------------------------------------------|
| `initializing`     | Starting up, connecting to the instrument.                 |
| `ready`            | Idle, waiting for a `measure` command.                     |
| `measuring`        | An XRF acquisition is in progress.                         |
| `measure_complete` | Acquisition finished, the result is being saved to disk.   |

Transitions:

- `initializing` → `ready` — once the measurer is ready (connection/login OK).
- `ready` → `measuring` — on receiving `measure <object_name>`.
- `measuring` → `measure_complete` — when the measurement is taken.
- `measure_complete` → `ready` — after the result file has been saved.

Commands received while not in `ready` (e.g. while already measuring) are
logged and ignored, so acquisitions are never run concurrently.

---

## MQTT topics & messages

Two topics are used (defaults shown; both are configurable):

| Role     | Default topic      | Direction | QoS |
|----------|--------------------|-----------|-----|
| Status   | `vanta/status`     | publish   | 1   |
| Command  | `commands/vanta`   | subscribe | 1   |

### Status message (published every 2 s)

A compact JSON object:

```json
{"state": "ready", "object": null, "timestamp": 1791214555.820288}
```

* `state` — one of `initializing`, `ready`, `measuring`, `measure_complete`.
* `object` — the name of the object currently being measured (or the last one),
  `null` until the first measurement.
* `timestamp` — Unix time of the status update.

### Command message (received)

```
measure <object_name>
```

The object name may be bare or quoted (single or double) and may contain
spaces — parsing uses `shlex`, so all of the following are accepted:

```
measure sample_01
measure 'sample_01'
measure "obj with spaces"
```

---

## Dependencies

The app uses the same dependencies as the shared MQTT client, plus one more
only when driving a real instrument:

```bash
# required (shared client)
pip install paho-mqtt pyyaml

# only for the real Vanta device (WebSocket)
pip install websocket-client
```

A running MQTT broker is also required. For local testing, a
[Mosquitto](https://mosquitto.org/) broker works well:

```bash
mosquitto -d -p 1883
```

---

## Quick start (simulation, no hardware)

Run the app with the built-in simulator — no Vanta device needed:

```bash
# 1. start a local broker (if you don't already have one)
mosquitto -d -p 1883

# 2. run the application
python vanta_mqtt_app.py
```

You will see the status being published every 2 seconds, starting in
`initializing` and settling into `ready`. Then, from another terminal, send a
measure command:

```bash
mosquitto_pub -t "commands/vanta" -m "measure 'sample_01'"
```

The app transitions `ready` → `measuring` → `measure_complete` → `ready` and
writes the result to `measures/sample_01.json`.

Stop the app with `Ctrl+C`.

---

## Running against a real Vanta

To drive a real instrument, select the `usb` (OTG) or `lan` backend:

```bash
# USB / OTG (device at its default address)
python vanta_mqtt_app.py --device usb

# LAN, with an explicit IP
python vanta_mqtt_app.py --device lan --ip 192.168.7.2
```

This uses the `VantaMeasurer`, which wraps
[`vanta_api_client.VantaClient`](vanta_api_client.py) (WebSocket connect,
login, start/stop test, heartbeat and result notification). See
[Notes on the real-device backend](#notes-on-the-real-device-backend) for
caveats.

You can also point the app at an existing broker and rename the topics:

```bash
python vanta_mqtt_app.py \
    --broker 192.168.1.50 --port 1883 \
    --status-topic vanta/status --command-topic commands/vanta
```

---

## CLI options

| Flag                   | Default          | Description                                            |
|------------------------|------------------|--------------------------------------------------------|
| `--config PATH`        | —                | Load the MQTT config from a YAML file (see `load_config`). Overrides the broker/topic flags below. |
| `--broker HOST`        | `localhost`      | MQTT broker address.                                   |
| `--port PORT`          | `1883`           | MQTT broker port.                                       |
| `--status-topic TOPIC` | `vanta/status`   | Topic used to publish the state.                        |
| `--command-topic TOPIC`| `commands/vanta` | Topic used to receive `measure <object>` commands.      |
| `--measures-dir DIR`   | `./measures`     | Directory where measurement files are saved.            |
| `--device {sim,usb,lan}` | `sim`          | Measurement backend: simulator, or real Vanta via USB/LAN. |
| `--ip HOST`            | —                | Vanta device IP (for `--device usb`/`lan`).             |
| `--measure-duration S` | `3.0`            | Simulated acquisition duration, in seconds (sim only).  |

`Ctrl+C` performs a graceful shutdown (status loop stopped, MQTT client and
device connection closed).

---

## Saved measurements

Each completed measurement is written as a JSON file named after the object:

```
measures/<object_name>.json
```

(Characters that are invalid in a file name are replaced with `_`.)

Simulated result example:

```json
{
  "object": "sample_01",
  "simulated": true,
  "timestamp": "2026-10-05T17:36:26.601778",
  "duration_s": 3.0,
  "elements_pct": {
    "Fe": 1.395,
    "Cu": 2.342,
    "Zn": 3.646,
    "Pb": 2.016,
    "As": 2.670,
    "Sb": 1.455,
    "Hg": 3.149,
    "Cd": 3.184
  }
}
```

With the real device the `result` field contains the instrument's raw result
payload and `simulated` is `false`.

---

## Testing / monitoring

Publish a command:

```bash
mosquitto_pub -t "commands/vanta" -m "measure 'sample_01'"
```

Watch the status stream:

```bash
mosquitto_sub -t "vanta/status" -v
```

Expected sequence for a single command:

```
{"state": "ready", ...}            (repeats every 2 s)
{"state": "measuring", "object": "sample_01", ...}
{"state": "measure_complete", "object": "sample_01", ...}
{"state": "ready", "object": "sample_01", ...}   (repeats every 2 s)
```

---

## Project layout

```
applications/Vanta xrf/
├── README.md              # this file
├── vanta_api_client.py    # Vanta XRF API client (WebSocket, connect/login/measure)
├── vanta_mqtt_app.py      # MQTT state-machine application (this example)
└── measures/              # created at runtime; saved measurements (one .json per object)
```

The shared MQTT client lives at the project root:

```
mqtt_client.py             # MQTTClient / MQTTConfig / TopicConfig / load_config
```

`vanta_mqtt_app.py` adds the project root to `sys.path` automatically, so you
can run it directly from this folder:

```bash
python vanta_mqtt_app.py
```

---

## Notes on the real-device backend

`VantaMeasurer` is a best-effort integration on top of `VantaClient`:

* `start()` — discovers the device, opens the WebSocket, logs in
  (`Administrator` / `0000`) and clears faults.
* `measure()` — starts the test (`commandId 601`), then polls for the result
  notification (`commandId 403`, `id 206`), sending a heartbeat every poll.
* `stop()` — closes the WebSocket.

A few things to keep in mind when using real hardware:

* The instrument must be reachable and logged in for an acquisition to
  complete; `start()` raises if any step fails (the app then goes to `ready`).
* `measure()` waits up to `timeout` seconds (default 120 s) for the result and
  stops the test if it times out.
* The login credentials and notification IDs mirror those in
  `vanta_api_client.py`; adjust there if your instrument differs.

The simulator backend (`--device sim`, the default) is fully self-contained and
is intended for development, demos and CI — it produces plausible element
concentrations and honors `--measure-duration`.
