# OCR-To-HA

OCR-To-HA captures still frames from multiple RTSP cameras, reads user-selected screen regions with local RapidOCR (ONNX Runtime) and Tesseract fallback, and publishes retained sensor values and Home Assistant MQTT Discovery configuration. Frames and OCR run inside the application container; they are not sent to an external service. The web server listens on all interfaces for LAN access.

## Install as a Home Assistant app

In Home Assistant, open **Settings → Apps → App store → ⋮ → Repositories** and add `https://github.com/spilegi/OCR-To-HA`. Find **OCR-To-HA** in the repository, install it, and start it. Open its **Open Web UI** button to use the interface through Home Assistant ingress. The app stores its configuration and feed definitions in its persistent `/data` volume, which is included in Home Assistant backups. Add RTSP camera details and MQTT broker settings in the app UI; for the built-in Mosquitto broker, the host is commonly `core-mosquitto`.

The Home Assistant app currently supports `amd64` and `aarch64` installations. The app needs outbound network access to the camera and MQTT broker. If Home Assistant cannot reach a camera on your network, check the host's routing and firewall rules. The ingress interface is protected by Home Assistant authentication; do not expose the standalone port directly to untrusted networks.

## Start with Docker Compose

Install Docker Engine and the Docker Compose plugin, then from this directory run:

```sh
docker compose up -d --build
```

Open `http://<docker-host-ip>:8000` on the LAN. Set `OCR_TO_HA_PORT` in a `.env` file to change the host port (for example, `OCR_TO_HA_PORT=8080`). `TZ` optionally sets the container timezone; it defaults to UTC. Compose publishes the application port and mounts the named persistent volume `ocr_to_ha_data` at `/data`. Feed definitions, RTSP URLs, and MQTT credentials are stored in `/data/config.json`; the file is written with owner-only permissions. Back it up by stopping the container and copying the volume, or with `docker cp ocr-to-ha:/data/config.json ./config-backup.json`. Protect backups as they contain credentials. Restore the file to `/data/config.json` and restart.

The service needs network access to the RTSP cameras and MQTT broker. Host networking is not required; ensure camera and broker addresses are reachable from the Docker network. Do not expose port 8000 publicly without adding authentication and TLS at a trusted reverse proxy. The application UI is intended for a trusted LAN and does not currently implement user authentication.

## Configure MQTT and Home Assistant

In the web UI enter the broker host and port, optional credentials, a discovery topic prefix (default `homeassistant`), and enable TLS if the broker requires it. Save settings and use **Test connection** to check broker reachability. A blank password retains the currently saved password; passwords are not returned by the API or shown after submission. MQTT over TCP is supported; TLS uses the system certificate store.

Home Assistant must have its MQTT integration connected to this broker, with discovery enabled for the configured prefix. For each feed, OCR-To-HA publishes one device and one sensor entity for each region. Discovery configs are retained under `<prefix>/sensor/<stable-id>/config`; state values are retained under `<prefix>/ocr_to_ha/<feed-id>/<region-id>/state`. A retained feed availability topic is published under `<prefix>/ocr_to_ha/<feed-id>/availability`. Entity IDs derive from persistent identifiers and remain stable when names or settings change. Deleting a feed clears its retained discovery configs. Changes made to a region's name or unit update its discovery config at its next OCR publish. The application never publishes RTSP URLs, credentials, or image data over MQTT.

## Add feeds and regions

Add a feed with a name and separate RTSP host/IP, port, username, password, and stream path fields (for example port `554`, path `/stream1`). The complete URL is treated as sensitive and stored in persistent config; the UI exposes editable components but never returns the saved password. Passwords are visible while typing and blank when editing; enter a replacement to change it or leave blank to keep the saved password. Feed errors omit URL credentials. Set enabled state, rotation (0–359° in 1° steps, previewed immediately), capture interval, and OCR interval. Capture and OCR intervals are independent, have a two-second minimum, and can be adjusted per feed.

Once a frame appears, open the feed editor and draw rectangular regions on the rotated preview. Set a sensor name, optional unit, and either whitespace trimming or numeric extraction. Per-region OCR controls select automatic/text/numeric recognition, image preprocessing (contrast enhancement, local contrast, sharpening, thresholding, or original), upscale factor (1–5), minimum OCR confidence (0–100%), and an optional allowed-character pattern. The pattern accepts fixed sequences of digits, digit groups (for example `[0-4]`), and a dot for the decimal separator; `[1][0-4].[0-9]` only accepts values from `10.0` through `14.9`. Matching Tesseract candidates are preferred and non-matches are not published. Optional inclusive minimum/maximum values also reject out-of-range OCR readings; rejected readings show the raw text in the app and publish `unavailable` instead of an incorrect number. Region coordinates scale proportionally with frame size. The region test button runs local OCR against the latest frame and displays the crop, recognized text, and confidence when available. The feed view shows connection status, recent capture/OCR timestamps, raw text, processed values, and errors. Preview refresh requests an immediate capture. Each feed maintains one RTSP-over-TCP session and samples still frames locally, avoiding repeated login attempts at each capture interval. Failed feeds retry independently with exponential backoff capped at 60 seconds; other feeds continue processing.

## Troubleshooting

- **No preview / RTSP errors:** Confirm the RTSP URL and camera stream profile, and check that the Docker host can reach the camera and port. For Tapo cameras, RTSP uses the camera account created in the Tapo app, not the TP-Link ID used to sign in to the app. If RTSP works in VLC but not here, compare the exact stream path and credentials; the app reports RTSP authentication rejections separately. Some cameras limit concurrent sessions. Feed errors omit URL credentials.
- **OCR is blank or inaccurate:** Check that the region is over the display text, include a little margin around the digits, adjust rotation, improve camera focus/exposure, and test the crop. RapidOCR uses ONNX versions of PaddleOCR models, with Tesseract as a fallback. For a fixed decimal display, the allowed-character pattern can restore a missing or misplaced decimal point only when the recognized digits fit every configured position; raw OCR remains visible, and readings that do not fit are withheld.
- **MQTT test fails:** Check broker address/port reachability from the container, username/password, TLS setting, and broker logs. Local capture/OCR continue while the broker is unavailable; publishing retries on later OCR cycles.
- **Entities do not appear:** Confirm Home Assistant's MQTT integration uses this broker and that discovery is enabled for the same topic prefix. Check for broker ACL rules that block discovery, state, or availability topics.
- **Persistent data:** `docker compose down` preserves the named volume. `docker compose down -v` removes it; back up `/data/config.json` before removing volumes.

## Development

Run the same container setup with `docker compose up --build`; the web UI is available on `0.0.0.0:8000` in the container. The image installs RapidOCR, ONNX Runtime, Tesseract, and FFmpeg for local OCR and RTSP decoding.
