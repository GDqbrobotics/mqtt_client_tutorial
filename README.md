# MQTT Client for Python

A simple, configurable, and production-ready MQTT client library for Python applications. Designed to be easily integrated into your projects, even if you're not an MQTT expert.

## Quick Start

```bash
# Install dependencies
pip install paho-mqtt pyyaml

# Run with your config
python mqtt_client.py --config config.yaml

# Test with an embedded broker (no external broker needed)
python mqtt_client.py --test-with-broker --config config_test.yaml
```

---

## Table of Contents

- [Quick Start](#quick-start)
- [What is MQTT?](#what-is-mqtt)
- [How to Add This Client to Your Project](#how-to-add-this-client-to-your-project)
- [Configuration Guide](#configuration-guide)
- [Usage Examples](#usage-examples)
- [Integration Patterns](#integration-patterns)
- [Troubleshooting](#troubleshooting)

---

## What is MQTT?

MQTT (Message Queuing Telemetry Transport) is a lightweight messaging protocol designed for:
- IoT devices with limited resources
- Unreliable or slow networks
- Real-time data streaming

**Key Concepts:**

| Term | Meaning |
|------|---------|
| **Broker** | The central server that routes messages (e.g., Mosquitto, HiveMQ) |
| **Topic** | Like a chat room name (e.g., `sensors/temperature/room1`) |
| **QoS** | Quality of Service: 0=fire & forget, 1=at least once, 2=exactly once |
| **Publish** | Send a message to a topic |
| **Subscribe** | Receive messages from a topic |

---

## How to Add This Client to Your Project

This is the most important section. Here's how to integrate the MQTT client into your application.

### Step 1: Install Dependencies

```bash
pip install paho-mqtt pyyaml
```

### Step 2: Create a Configuration File

Create `config.yaml` in your project root:

```yaml
broker: "192.168.1.100"
port: 1883
client_id: "my_sensor_node_01"
keepalive: 60
clean_session: true
verbose: true

topics:
  - topic: "sensors/temperature"
    qos: 1
    direction: "receive"
  - topic: "commands/control"
    qos: 0
    direction: "send"
```

### Step 3: Add to Your Application

**Option A: Run as a standalone script (simplest)**

```python
# main.py
from mqtt_client import MQTTClient, load_config
import time

# Load configuration
config = load_config('config.yaml')

# Create client
client = MQTTClient(config)

# Start connection
if client.start():
    print("Connected to MQTT broker!")
    
    # Keep the process alive
    while True:
        time.sleep(1)
else:
    print("Failed to connect!")
```

**Option B: Integrate as a module (recommended for real projects)**

```python
# main.py
from mqtt_client import MQTTClient, MQTTConfig, TopicConfig
import time

def create_mqtt_config():
    """Build the MQTT configuration programmatically."""
    return MQTTConfig(
        broker="localhost",
        port=1883,
        client_id="my_app_client",
        keepalive=60,
        clean_session=True,
        verbose=True,
        topics=[
            TopicConfig(
                topic="sensors/temperature",
                qos=1,
                direction="receive"
            ),
            TopicConfig(
                topic="commands/control",
                qos=0,
                direction="send"
            )
        ]
    )

def on_message_received(client, userdata, msg):
    """Custom callback: process each incoming message."""
    payload = msg.payload.decode('utf-8', errors='replace')
    print(f"[{msg.topic}] -> {payload}")
    # Add your business logic here
    # Example: if msg.topic == "commands/control":
    #     handle_command(payload)

def main():
    config = create_mqtt_config()
    
    # Attach your custom message handler
    config.on_message = on_message_received
    
    client = MQTTClient(config)
    
    if client.start():
        print("MQTT connected. Application running...")
        
        # Your application logic goes here.
        # The client will keep receiving messages in the background.
        try:
            while True:
                # Do your application work
                time.sleep(5)
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            client.stop()
            print("MQTT client stopped cleanly.")

if __name__ == '__main__':
    main()
```

**Option C: Run as CLI (for quick tests)**

```bash
# Run with your config
python mqtt_client.py --config config.yaml

# Verbose mode
python mqtt_client.py --config config.yaml --verbose

# Test with embedded broker
python mqtt_client.py --test-with-broker --config config_test.yaml
```

---

## Configuration Guide

The `config.yaml` file controls every aspect of the client. Here's every setting:

### Connection Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `broker` | string | `localhost` | Address of the MQTT broker |
| `port` | int | `1883` | Broker port (1883=MQTT, 8883=MQTT over TLS) |
| `client_id` | string | *required* | Unique identifier for this client |
| `username` | string | *optional* | Broker username |
| `password` | string | *optional* | Broker password |
| `keepalive` | int | `60` | Keep-alive interval in seconds |
| `clean_session` | bool | `true` | Start fresh each connection |

### Topic Settings

Each entry in the `topics` list:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `topic` | string | *required* | MQTT topic name |
| `qos` | int | `1` | Quality of Service (0, 1, or 2) |
| `retain` | bool | `false` | Retain message on broker |
| `direction` | string | `both` | `send`, `receive`, or `both` |
| `message_prefix` | string | `''` | Optional prefix for messages |
| `message_suffix` | string | `''` | Optional suffix for messages |

### Behavioral Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `verbose` | bool | `true` | Print all messages to screen |
| `reconnect_delay` | int | `5` | Initial reconnect delay in seconds |
| `max_reconnect_attempts` | int | `10` | Max reconnect attempts |

---

## Usage Examples

### Example 1: Simple Temperature Sensor

```yaml
# config.yaml
broker: "192.168.1.50"
port: 1883
client_id: "temp_sensor_01"
verbose: true

topics:
  - topic: "sensors/temperature"
    qos: 1
    direction: "receive"
  
  - topic: "sensors/temperature/status"
    qos: 0
    direction: "send"
```

```python
# sensor.py
from mqtt_client import MQTTClient, load_config
import time

def main():
    config = load_config('config.yaml')
    client = MQTTClient(config)
    
    if client.start():
        print("Temperature sensor connected!")
        
        # Simulate reading temperature
        temp = 23.5
        client.publish('sensors/temperature/status', f'{temp}°C', qos=0)
        
        while True:
            time.sleep(1)

if __name__ == '__main__':
    main()
```

### Example 2: Command & Control System

```yaml
# config.yaml
broker: "localhost"
port: 1883
client_id: "control_system_01"
verbose: true

topics:
  - topic: "commands/start"
    qos: 1
    direction: "receive"
  - topic: "commands/stop"
    qos: 1
    direction: "receive"
  - topic: "system/status"
    qos: 1
    direction: "send"
```

```python
# control.py
from mqtt_client import MQTTClient, load_config
import time

def handle_command(payload):
    """Process incoming commands."""
    cmd = payload.strip().lower()
    if cmd == 'start':
        print(">>> START command received")
        # Your logic here
    elif cmd == 'stop':
        print(">>> STOP command received")
        # Your logic here
    else:
        print(f">>> Unknown command: {cmd}")

def main():
    config = load_config('config.yaml')
    
    # Set custom handler
    config.on_message = lambda client, userdata, msg: handle_command(msg.payload.decode())
    
    client = MQTTClient(config)
    
    if client.start():
        print("Control system ready.")
        client.publish('system/status', 'ONLINE', qos=0)
        
        while True:
            time.sleep(1)

if __name__ == '__main__':
    main()
```

### Example 3: Multi-Topic Dashboard

```yaml
# config.yaml
broker: "dashboard.local"
port: 8883
client_id: "dashboard_01"
verbose: true

topics:
  - topic: "sensors/temperature"
    qos: 1
    direction: "receive"
  - topic: "sensors/humidity"
    qos: 1
    direction: "receive"
  - topic: "alerts/critical"
    qos: 2
    direction: "receive"
  - topic: "dashboard/logs"
    qos: 0
    direction: "send"
    message_prefix: "[DASHBOARD]"
```

```python
# dashboard.py
from mqtt_client import MQTTClient, load_config
import time

class Dashboard:
    def __init__(self):
        self.config = load_config('config.yaml')
        self.config.on_message = self.handle_message
        self.client = MQTTClient(self.config)
        self.metrics = {}
    
    def handle_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()
        
        if 'temperature' in topic:
            self.metrics['temp'] = float(payload)
        elif 'humidity' in topic:
            self.metrics['humidity'] = float(payload)
        elif 'critical' in topic:
            print(f"🚨 ALERT: {payload}")
    
    def start(self):
        if self.client.start():
            print("Dashboard connected!")
            while True:
                time.sleep(1)
    
    def stop(self):
        self.client.stop()

if __name__ == '__main__':
    d = Dashboard()
    d.start()
```

---

## Integration Patterns

### Pattern 1: Background Thread (for long-running apps)

```python
import threading
from mqtt_client import MQTTClient, MQTTConfig, TopicConfig

def mqtt_worker(config, ready_event):
    """Run MQTT client in a background thread."""
    client = MQTTClient(config)
    client.start()
    ready_event.set()
    
    # Keep alive
    while True:
        time.sleep(1)

# In your main app:
config = MQTTConfig(broker="localhost", client_id="app", topics=[])
ready = threading.Event()
mqtt_thread = threading.Thread(target=mqtt_worker, args=(config, ready), daemon=True)
mqtt_thread.start()
ready.wait(timeout=30)
```

### Pattern 2: Graceful Shutdown

```python
import signal
from mqtt_client import MQTTClient, load_config

def make_graceful(config):
    client = MQTTClient(config)
    
    def shutdown(signum, frame):
        print("Shutting down MQTT...")
        client.stop()
        signal.default_int_handler(signum, frame)
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    return client

# Usage:
config = load_config('config.yaml')
client = make_graceful(config)
client.start()
```

### Pattern 3: Message Routing

```python
from mqtt_client import MQTTClient, load_config

def route_message(client, userdata, msg):
    """Route messages to different handlers based on topic."""
    topic = msg.topic
    payload = msg.payload.decode()
    
    if topic.startswith('sensors/'):
        handle_sensor(payload)
    elif topic.startswith('commands/'):
        handle_command(payload)
    else:
        handle_default(payload)

def handle_sensor(payload):
    print(f"Sensor data: {payload}")

def handle_command(payload):
    print(f"Command: {payload}")

def handle_default(payload):
    print(f"Unknown: {payload}")

# Usage:
config = load_config('config.yaml')
config.on_message = route_message
client = MQTTClient(config)
client.start()
```

---

## Troubleshooting

### Connection Refused

**Error:** `Connection refused` or `ConnectionError`

**Solutions:**
1. Check your broker address is correct: `netstat -tlnp | grep 1883`
2. Verify the broker is running: `mosquitto -d` or `mosquitto -c /etc/mosquitto/mosquitto.conf`
3. Check firewall: `sudo ufw status`
4. Verify your `client_id` is unique

### Authentication Failed

**Error:** `Connection refused` after providing credentials

**Solutions:**
1. Check `username` and `password` in config
2. Verify broker has auth enabled: `mosquitto -c /etc/mosquitto/mosquitto.conf`
3. Check broker auth file: `/etc/mosquitto/passwd`

### Message Not Received

**Issue:** Client connected but no messages arriving

**Solutions:**
1. Verify topic is correct (no typos)
2. Check `direction` is set to `receive` or `both`
3. Verify QoS level is appropriate
4. Check broker logs for errors

### Reconnection Loop

**Issue:** Client keeps reconnecting

**Solutions:**
1. Increase `keepalive` value
2. Check network connectivity
3. Verify broker is reachable
4. Check `max_reconnect_attempts`

---

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

### Development Setup

```bash
git clone https://github.com/GDqbrobotics/mqtt-client-tutorial
cd mqtt-client-tutorial
pip install -r requirements.txt
python test_client.py --config config_test.yaml
```


If you're stuck:

1. Check the troubleshooting section above
2. Verify your broker is running
3. Test with `--test-with-broker` flag
4. Check your `config.yaml` syntax

For help, open an issue on GitHub.

---

## Key Takeaways

1. **Config-driven**: All settings are in `config.yaml` — no code changes needed
2. **Easy to integrate**: Just `import mqtt_client` and go
3. **Scalable**: Add more topics by editing the config file
4. **Verbose by default**: See every message in your terminal
5. **Testable**: `--test-with-broker` runs a local broker for development
