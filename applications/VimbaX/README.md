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

Send either `measure 'sample 01'` or a command with the camera pose
(x, y, z, qx, qy, qz, qw) and the waypoint number:

```bash
mosquitto_pub -t vimbax/commands \
  -m 'acquire: object_1; camera pose: [0.4, 0.1, 0.6, 0, 0, 0, 1]; waypoint_1'
```

The state is published on `vimbax/status` as JSON, every 2 s and on every
change: `{"state": "measure_complete", "object": "object_1", "waypoint": 1, ...}`.

Each acquisition is saved beneath `applications/VimbaX/measures/<object>/` as
a `.jpg` and `.json` pair named `<object>_wp_<n>` when the command has a
waypoint (a new scan of the same object overwrites it), otherwise
`<object>_<timestamp>`.