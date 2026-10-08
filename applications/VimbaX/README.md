## VimbaX Camera Application

The VimbaX example publishes camera status over MQTT and saves each captured
frame as a JPEG with matching JSON metadata. Install the project dependencies
with `pip install -r requirements.txt`. Simulation mode does not require a
camera:

```bash
python applications/VimbaX/vimbax_mqtt_app.py --device sim
```

For a real camera, install the Vimba X API with `pip install '/path/to/vimbax_api/vmbpy-X.Y.Z-py-none-any.whl`, then run:

```bash
python applications/VimbaX/vimbax_mqtt_app.py --device real [--camera-id CAMERA_ID]
```

Send either `measure 'sample 01'` or a command with a six-value camera position:

```bash
mosquitto_pub -t commands/vimbax \
  -m 'acquire: "sample 01"; camera position: [0, 0, 0, 0, 0, 0]'
```

Each acquisition is saved beneath `applications/VimbaX/measures/<object>/` as
a timestamped `.jpg` and `.json` pair.