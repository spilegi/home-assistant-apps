import asyncio
import base64
import json
import logging
import math
import os
import re
import select
import subprocess
import threading
import time
import uuid
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import cv2
import numpy as np
import paho.mqtt.client as mqtt
import pytesseract
from rapidocr import RapidOCR
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ocr-to-ha")
cv2.setLogLevel(3)
DATA = Path(os.getenv("OCR_TO_HA_DATA", "/data"))
CONFIG = DATA / "config.json"
DATA.mkdir(parents=True, exist_ok=True)
lock = threading.RLock()
states = {}
frames = {}
raw_frames = {}
refresh_events = {}
ocr_engine = None
ocr_engine_lock = threading.Lock()
stop_event = threading.Event()
mqtt_service_lock = threading.Lock()
mqtt_service_cache = {"checked_at": 0.0, "value": None}


def now():
    return datetime.now(timezone.utc).isoformat()


def default_config():
    return {"mqtt": {"host": "", "port": 1883, "username": "", "password": "", "topic_prefix": "homeassistant", "tls": False}, "feeds": []}


def load_config():
    try:
        value = json.loads(CONFIG.read_text())
        if not isinstance(value.get("feeds"), list) or not isinstance(value.get("mqtt"), dict):
            raise ValueError("invalid configuration")
        return value
    except FileNotFoundError:
        return default_config()
    except Exception:
        log.exception("Could not load config; starting with empty configuration")
        return default_config()


config = load_config()


def save_config():
    temporary = CONFIG.with_suffix(".tmp")
    temporary.write_text(json.dumps(config, indent=2))
    os.chmod(temporary, 0o600)
    temporary.replace(CONFIG)


def safe_url(url):
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        if parts.port:
            host += f":{parts.port}"
        return urlunsplit((parts.scheme, host, parts.path, "", ""))
    except Exception:
        return "<redacted RTSP URL>"


def public_feed(feed):
    result = {k: v for k, v in feed.items() if k != "url"}
    result["url_set"] = bool(feed.get("url"))
    if feed.get("url"):
        parts = urlsplit(feed["url"])
        result["rtsp"] = {"host": parts.hostname or "", "port": parts.port or 554, "username": unquote(parts.username or ""), "path": parts.path or "/", "password_set": bool(parts.password)}
    else:
        result["rtsp"] = {"host": "", "port": 554, "username": "", "path": "/stream1", "password_set": False}
    return result


def public_mqtt():
    effective, source = mqtt_connection_settings()
    result = {k: v for k, v in effective.items() if k != "password"}
    result["password_set"] = bool(effective.get("password"))
    result["source"] = source
    return result


def homeassistant_mqtt_service():
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token:
        return None
    current_time = time.monotonic()
    with mqtt_service_lock:
        if current_time - mqtt_service_cache["checked_at"] < 30:
            value = mqtt_service_cache["value"]
            return value.copy() if value else None
        value = None
        request = urllib.request.Request(
            "http://supervisor/services/mqtt",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                service = json.load(response)
            host = service.get("host")
            port = int(service.get("port", 1883))
            if host and 1 <= port <= 65535:
                value = {
                    "host": host,
                    "port": port,
                    "username": service.get("username", ""),
                    "password": service.get("password", ""),
                    "tls": bool(service.get("ssl", False)),
                }
        except (OSError, TypeError, ValueError, urllib.error.URLError, json.JSONDecodeError):
            value = None
        mqtt_service_cache.update({"checked_at": current_time, "value": value})
        return value.copy() if value else None


def mqtt_connection_settings():
    with lock:
        manual = dict(config["mqtt"])
    service = homeassistant_mqtt_service()
    if not service:
        return manual, "manual"
    return {**manual, **service}, "home_assistant"


def public_status(feed_id):
    status = dict(states.get(feed_id, {}))
    status.pop("url", None)
    return status


class FeedInput(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    url: str | None = None
    rtsp_host: str | None = None
    rtsp_port: int | None = Field(default=None, ge=1, le=65535)
    rtsp_username: str | None = None
    rtsp_password: str | None = None
    rtsp_path: str | None = None
    clear_rtsp_password: bool = False
    enabled: bool = True
    rotation: int = 0
    capture_interval: int = Field(default=10, ge=2, le=3600)
    ocr_interval: int = Field(default=15, ge=2, le=3600)
    regions: list[dict] = Field(default_factory=list)

    @field_validator("rotation")
    @classmethod
    def valid_rotation(cls, value):
        if value < 0 or value > 359:
            raise ValueError("Rotation must be between 0 and 359 degrees")
        return value

    @field_validator("url")
    @classmethod
    def valid_url(cls, value):
        if value and not value.lower().startswith("rtsp://"):
            raise ValueError("URL must start with rtsp://")
        return value


def compose_rtsp_url(value, existing=""):
    if value.get("url"):
        return value["url"]
    host = (value.get("rtsp_host") or "").strip()
    if not host:
        return existing or ""
    if any(char in host for char in "@/?#"):
        raise HTTPException(422, "Enter only the camera host or IP address")
    try:
        parsed_host = urlsplit("//" + host)
        if not parsed_host.hostname or parsed_host.port:
            raise ValueError()
    except ValueError:
        raise HTTPException(422, "Enter a valid camera host or IP address")
    previous = urlsplit(existing) if existing else None
    username = (value.get("rtsp_username") or "").strip()
    password = value.get("rtsp_password") or ""
    if not password and previous and not value.get("clear_rtsp_password"):
        password = unquote(previous.password or "")
    if not username and previous:
        username = unquote(previous.username or "")
    auth = quote(username, safe="") + (":" + quote(password, safe="") if password else "")
    netloc = (auth + "@" if auth else "") + host
    port = value.get("rtsp_port") or (previous.port if previous else 554) or 554
    path = (value.get("rtsp_path") or (previous.path if previous else "/stream1") or "/stream1").strip()
    if not path.startswith("/"):
        path = "/" + path
    if any(char in path for char in "?#"):
        raise HTTPException(422, "Stream path cannot contain query or fragment characters")
    return urlunsplit(("rtsp", f"{netloc}:{port}", path, "", ""))


class MQTTInput(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=1883, ge=1, le=65535)
    username: str = Field(default="", max_length=255)
    password: str = Field(default="", max_length=1024)
    topic_prefix: str = Field(default="homeassistant", min_length=1, max_length=128)
    tls: bool = False

    @field_validator("topic_prefix")
    @classmethod
    def valid_prefix(cls, value):
        if any(c in value for c in "+#\x00") or value.startswith("/") or value.endswith("/"):
            raise ValueError("Topic prefix cannot contain +, #, null, or leading/trailing slashes")
        return value


def rotate(image, degrees):
    if degrees % 360 == 0:
        return image
    cardinal_rotations = {90: cv2.ROTATE_90_CLOCKWISE, 180: cv2.ROTATE_180, 270: cv2.ROTATE_90_COUNTERCLOCKWISE}
    if degrees in cardinal_rotations:
        return cv2.rotate(image, cardinal_rotations[degrees])
    height, width = image.shape[:2]
    radians = math.radians(degrees)
    cosine, sine = math.cos(radians), math.sin(radians)
    cosine = 0 if abs(cosine) < 1e-12 else cosine
    sine = 0 if abs(sine) < 1e-12 else sine
    output_width = math.ceil(abs(cosine) * width + abs(sine) * height)
    output_height = math.ceil(abs(sine) * width + abs(cosine) * height)
    matrix = cv2.getRotationMatrix2D((width / 2, height / 2), -degrees, 1)
    matrix[0, 2] += (output_width - width) / 2
    matrix[1, 2] += (output_height - height) / 2
    return cv2.warpAffine(image, matrix, (output_width, output_height), flags=cv2.INTER_LINEAR, borderMode=cv2.BORDER_CONSTANT)


def encode_preview(image):
    ok, data = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 80])
    if not ok:
        raise ValueError("Could not encode image")
    return "data:image/jpeg;base64," + base64.b64encode(data).decode()


def read_mjpeg_frame(process, buffer, timeout):
    deadline = time.monotonic() + timeout
    while True:
        start = buffer.find(b"\xff\xd8")
        if start >= 0:
            end = buffer.find(b"\xff\xd9", start + 2)
            if end >= 0:
                return buffer[start:end + 2], buffer[end + 2:], False
            buffer = buffer[start:]
        elif len(buffer) > 1:
            buffer = buffer[-1:]
        remaining = max(0, deadline - time.monotonic())
        ready, _, _ = select.select([process.stdout], [], [], remaining)
        if not ready:
            return None, buffer, process.poll() is not None
        chunk = os.read(process.stdout.fileno(), 65536)
        if not chunk:
            return None, buffer, True
        buffer += chunk


def clean_text(raw, rule):
    value = raw.strip()
    if rule == "numeric":
        match = re.search(r"[-+]?\d+(?:[.,]\d+)?", value)
        return match.group(0).replace(",", ".") if match else value
    return value


def normalize_expected_pattern(expected_format):
    expected_format = str(expected_format or "").strip()
    if not expected_format:
        return ""
    if re.fullmatch(r"X+(?:\.X+)?", expected_format):
        return expected_format.replace("X", r"\d").replace(".", r"\.")
    if len(expected_format) > 100:
        raise ValueError("Expected pattern is too long")
    tokens = []
    index = 0
    while index < len(expected_format):
        char = expected_format[index]
        if char == "\\":
            if index + 1 >= len(expected_format) or expected_format[index + 1] != ".":
                raise ValueError("Only an escaped decimal point is supported outside digit groups")
            tokens.append(r"\.")
            index += 2
        elif char == ".":
            tokens.append(r"\.")
            index += 1
        elif char == "[":
            end = expected_format.find("]", index + 1)
            if end < 0:
                raise ValueError("Digit groups must have a closing bracket")
            group = expected_format[index + 1:end].replace(",", "")
            if not group or not re.fullmatch(r"[0-9-]+", group):
                raise ValueError("Digit groups can contain digits, commas, and ranges such as [0-4]")
            tokens.append(f"[{group}]")
            index = end + 1
        elif char.isdigit():
            tokens.append(char)
            index += 1
        else:
            raise ValueError("Use digits, digit groups like [0-4], and a decimal point only")
    pattern = "".join(tokens)
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError("Invalid digit range in expected pattern") from exc
    return pattern


def matches_expected_format(value, expected_format):
    if not expected_format:
        return True
    value = clean_text(value, "numeric")
    return re.fullmatch(normalize_expected_pattern(expected_format), value) is not None


def restore_expected_decimal(raw, expected_format):
    pattern = normalize_expected_pattern(expected_format)
    if pattern.count(r"\.") != 1 or not re.fullmatch(r"\s*\+?\d[\d\s.,]*\s*", raw or ""):
        return None
    left_pattern, right_pattern = pattern.split(r"\.")
    digits = "".join(re.findall(r"\d", raw))
    parts = []
    for part in (left_pattern, right_pattern):
        slots = []
        index = 0
        while index < len(part):
            if part.startswith(r"\d", index):
                slots.append(r"\d")
                index += 2
            elif part[index] == "[":
                end = part.index("]", index)
                slots.append(part[index:end + 1])
                index = end + 1
            else:
                slots.append(part[index])
                index += 1
        parts.append(slots)
    slots = parts[0] + parts[1]
    if len(digits) != len(slots) or any(not re.fullmatch(slot, digit) for slot, digit in zip(slots, digits)):
        return None
    decimal_index = len(parts[0])
    return f"{digits[:decimal_index]}.{digits[decimal_index:]}"


def expected_character_whitelist(expected_format):
    pattern = normalize_expected_pattern(expected_format)
    if not pattern:
        return "0123456789.,+-"
    allowed = set()
    index = 0
    while index < len(pattern):
        if pattern.startswith(r"\d", index):
            allowed.update("0123456789")
            index += 2
        elif pattern.startswith(r"\.", index):
            allowed.add(".")
            index += 2
        elif pattern[index] == "[":
            end = pattern.index("]", index)
            group = pattern[index + 1:end]
            position = 0
            while position < len(group):
                if position + 2 < len(group) and group[position + 1] == "-":
                    allowed.update(chr(code) for code in range(ord(group[position]), ord(group[position + 2]) + 1))
                    position += 3
                else:
                    allowed.add(group[position])
                    position += 1
            index = end + 1
        else:
            allowed.add(pattern[index])
            index += 1
    return "".join(char for char in "0123456789." if char in allowed)


def mqtt_client(timeout=5):
    cfg, _ = mqtt_connection_settings()
    if not cfg.get("host"):
        raise ValueError("Configure an MQTT broker first")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="ocr-to-ha-" + uuid.uuid4().hex[:8])
    if cfg.get("username"):
        client.username_pw_set(cfg["username"], cfg.get("password") or None)
    if cfg.get("tls"):
        client.tls_set()
    connected = threading.Event()
    result = {"error": None}

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            result["error"] = str(reason_code)
        connected.set()

    client.on_connect = on_connect
    client.connect(cfg["host"], cfg.get("port", 1883), keepalive=30)
    client.loop_start()
    if not connected.wait(timeout) or result["error"]:
        client.loop_stop()
        client.disconnect()
        raise ConnectionError(result["error"] or "Connection timed out")
    return client


def discovery(client, feed):
    prefix = config["mqtt"]["topic_prefix"]
    device_id = "ocr_to_ha_" + feed["id"]
    availability = f"{prefix}/ocr_to_ha/{feed['id']}/availability"
    for region in feed.get("regions", []):
        sensor_id = f"{device_id}_{region['id']}"
        topic = f"{prefix}/sensor/{sensor_id}/config"
        payload = {"name": region["name"], "unique_id": sensor_id, "state_topic": f"{prefix}/ocr_to_ha/{feed['id']}/{region['id']}/state", "availability_topic": availability, "payload_available": "online", "payload_not_available": "offline", "device": {"identifiers": [device_id], "name": f"OCR-To-HA {feed['name']}", "manufacturer": "OCR-To-HA", "model": "Local OCR camera feed"}}
        if region.get("unit"):
            payload["unit_of_measurement"] = region["unit"]
        client.publish(topic, json.dumps(payload), qos=1, retain=True)
    client.publish(availability, "online", qos=1, retain=True)


def clear_discovery(feed_id, region_ids):
    try:
        client = mqtt_client(timeout=2)
        prefix = config["mqtt"]["topic_prefix"]
        device_id = "ocr_to_ha_" + feed_id
        for region_id in region_ids:
            client.publish(f"{prefix}/sensor/{device_id}_{region_id}/config", "", qos=1, retain=True)
        client.disconnect()
        client.loop_stop()
    except Exception:
        pass


def publish_states(feed, results):
    try:
        client = mqtt_client(timeout=2)
        discovery(client, feed)
        prefix = config["mqtt"]["topic_prefix"]
        for region in feed.get("regions", []):
            if region["id"] in results:
                result = results[region["id"]]
                state = "unavailable" if result.get("error") or result.get("rejected") else result["value"]
                client.publish(f"{prefix}/ocr_to_ha/{feed['id']}/{region['id']}/state", state, qos=1, retain=True)
        client.disconnect()
        client.loop_stop()
        states.setdefault(feed["id"], {})["mqtt_error"] = None
    except Exception as exc:
        states.setdefault(feed["id"], {})["mqtt_error"] = str(exc)[:200]


def ocr_region(image, region):
    height, width = image.shape[:2]
    basis_w, basis_h = region.get("frame_width", width), region.get("frame_height", height)
    x = max(0, min(width, int(region.get("x", 0) * width / max(1, basis_w))))
    y = max(0, min(height, int(region.get("y", 0) * height / max(1, basis_h))))
    w = max(1, min(width - x, int(region.get("width", 1) * width / max(1, basis_w))))
    h = max(1, min(height - y, int(region.get("height", 1) * height / max(1, basis_h))))
    crop = image[y:y+h, x:x+w]
    if crop.size == 0:
        raise ValueError("Region lies outside the captured frame")
    ocr_input = preprocess_crop(crop, region)
    global ocr_engine
    if ocr_engine is None:
        with ocr_engine_lock:
            if ocr_engine is None:
                ocr_engine = RapidOCR()
    result = ocr_engine(ocr_input)
    raw = " ".join(result.txts or ()).strip()
    scores = getattr(result, "scores", None)
    scores = list(scores) if scores is not None else []
    confidence = round(sum(float(score) for score in scores) / len(scores) * 100, 1) if scores else None
    expected_format = region.get("expected_format", "")
    numeric_mode = region.get("ocr_mode", "auto") == "numeric" or region.get("rule") == "numeric" or region.get("min_value") is not None or region.get("max_value") is not None or bool(expected_format)
    if numeric_mode:
        alternate, alternate_confidence = tesseract_fallback(crop, region, numeric=True)
        rapid_matches = matches_expected_format(raw, expected_format)
        alternate_matches = matches_expected_format(alternate, expected_format)
        if alternate and ((alternate_matches and not rapid_matches) or (alternate_matches == rapid_matches and (not raw or (alternate_confidence is not None and (confidence is None or alternate_confidence > confidence))))):
            raw, confidence = alternate, alternate_confidence
    elif not raw:
        raw, confidence = tesseract_fallback(crop, region)
    minimum = region.get("min_value")
    maximum = region.get("max_value")
    value = clean_text(raw, "numeric" if numeric_mode or minimum is not None or maximum is not None else region.get("rule"))
    format_applied = False
    if expected_format and not matches_expected_format(value, expected_format):
        repaired_value = restore_expected_decimal(raw, expected_format)
        if repaired_value is not None:
            value = repaired_value
            format_applied = True
    result = {"raw": raw, "value": value, "confidence": confidence, "format_applied": format_applied, "crop": encode_preview(crop)}
    minimum_confidence = region.get("min_confidence", 0)
    if confidence is not None and confidence < minimum_confidence:
        result.update({"rejected": True, "error": f"OCR confidence {confidence:g}% is below the configured {minimum_confidence:g}% minimum; not published."})
    if expected_format and not matches_expected_format(value, expected_format):
        result.update({"rejected": True, "error": f"OCR result does not match the expected format {expected_format} (use X for each digit); not published."})
    if minimum is not None or maximum is not None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            result.update({"rejected": True, "error": f"No numeric value recognized (raw: '{raw}'); not published."})
        else:
            if (minimum is not None and number < minimum) or (maximum is not None and number > maximum):
                lower = f"{minimum:g}" if minimum is not None else "no minimum"
                upper = f"{maximum:g}" if maximum is not None else "no maximum"
                result.update({"rejected": True, "error": f"OCR value {value} (raw: '{raw}') is outside {lower}–{upper}; not published."})
    return result


def preprocess_crop(crop, region):
    scale = region.get("upscale", 3)
    interpolation = cv2.INTER_CUBIC if scale > 1 else cv2.INTER_AREA
    resized = cv2.resize(crop, None, fx=scale, fy=scale, interpolation=interpolation) if scale != 1 else crop.copy()
    mode = region.get("preprocess", "enhance")
    if mode == "original":
        return resized
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    if mode == "contrast":
        gray = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    elif mode == "sharpen":
        gray = cv2.filter2D(gray, -1, np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]]))
    elif mode == "threshold":
        gray = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    else:
        gray = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    return gray


def tesseract_fallback(crop, region, numeric=False):
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    scale = region.get("upscale", 3)
    if scale != 1:
        gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
    if region.get("preprocess") == "contrast":
        gray = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    elif region.get("preprocess") == "sharpen":
        gray = cv2.filter2D(gray, -1, np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]]))
    thresholded = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    candidates = []
    for source in (gray, thresholded):
        for page_mode in (6, 7):
            allowed_characters = expected_character_whitelist(region.get("expected_format")) if numeric and region.get("expected_format") else "0123456789.,+-"
            whitelist = f" -c tessedit_char_whitelist={allowed_characters}" if numeric else ""
            result = pytesseract.image_to_data(source, config=f"--psm {page_mode}{whitelist}", output_type=pytesseract.Output.DICT)
            words = [(word.strip(), float(confidence)) for word, confidence in zip(result["text"], result["conf"]) if word.strip() and float(confidence) >= 0]
            if words:
                candidates.append((sum(confidence for _, confidence in words) / len(words), " ".join(word for word, _ in words)))
    if not candidates:
        return "", None
    expected_format = region.get("expected_format", "")
    matching = [candidate for candidate in candidates if matches_expected_format(candidate[1], expected_format)] if expected_format else []
    confidence, text = max(matching or candidates, key=lambda candidate: candidate[0])
    return text, round(confidence, 1)


def feed_worker(feed_id):
    last_ocr = 0
    delay = 1
    while not stop_event.is_set():
        with lock:
            feed = next((f.copy() for f in config["feeds"] if f["id"] == feed_id), None)
        if not feed or not feed.get("enabled") or not feed.get("url"):
            time.sleep(1)
            continue
        process = None
        try:
            process = subprocess.Popen(
                ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-rtsp_transport", "tcp", "-i", feed["url"], "-an", "-vf", "fps=1", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            buffer = b""
            latest_jpeg = None
            last_frame_time = time.monotonic()
            next_capture = 0
            event = refresh_events.setdefault(feed_id, threading.Event())
            while not stop_event.is_set():
                with lock:
                    current_feed = next((item.copy() for item in config["feeds"] if item["id"] == feed_id), None)
                if not current_feed or not current_feed.get("enabled") or not current_feed.get("url"):
                    break
                if current_feed["url"] != feed["url"]:
                    break
                feed = current_feed
                jpeg, buffer, ended = read_mjpeg_frame(process, buffer, 1)
                if jpeg:
                    latest_jpeg = jpeg
                    last_frame_time = time.monotonic()
                    while True:
                        newer, buffer, ended = read_mjpeg_frame(process, buffer, 0)
                        if not newer:
                            break
                        latest_jpeg = newer
                        last_frame_time = time.monotonic()
                if ended:
                    details = process.stderr.read().decode(errors="replace").lower()
                    if "401" in details or "unauthorized" in details or "authorization failed" in details:
                        raise ConnectionError("Camera rejected RTSP authentication (401); check the camera account and RTSP access settings")
                    raise ConnectionError("RTSP stream ended before a frame could be read")
                if time.monotonic() - last_frame_time > 15:
                    raise ConnectionError("No frames received from RTSP stream for 15 seconds")
                current_time = time.monotonic()
                refresh_requested = event.is_set()
                if latest_jpeg and (current_time >= next_capture or refresh_requested):
                    event.clear()
                    raw_image = cv2.imdecode(np.frombuffer(latest_jpeg, dtype=np.uint8), cv2.IMREAD_COLOR)
                    latest_jpeg = None
                    if raw_image is None:
                        raise ConnectionError("Could not decode a frame from the RTSP stream")
                    image = rotate(raw_image, feed.get("rotation", 0))
                    with lock:
                        raw_frames[feed_id] = raw_image
                        frames[feed_id] = image
                    stamp = now()
                    status = states.setdefault(feed_id, {})
                    status.update({"status": "connected", "last_capture": stamp, "error": None})
                    if current_time - last_ocr >= feed.get("ocr_interval", 15):
                        results = {}
                        for region in feed.get("regions", []):
                            try:
                                results[region["id"]] = ocr_region(image, region)
                                results[region["id"]]["timestamp"] = now()
                            except Exception as exc:
                                results[region["id"]] = {"error": str(exc)[:200]}
                        status.update({"last_ocr": now(), "results": results})
                        last_ocr = current_time
                        publish_states(feed, results)
                    delay = 1
                    next_capture = current_time + feed.get("capture_interval", 10)
        except Exception as exc:
            message = str(exc)
            states.setdefault(feed_id, {}).update({"status": "error", "error": message[:200], "retry_in": delay})
            if delay >= 8:
                try:
                    client = mqtt_client(timeout=2)
                    client.publish(f"{config['mqtt']['topic_prefix']}/ocr_to_ha/{feed_id}/availability", "offline", qos=1, retain=True)
                    client.disconnect()
                    client.loop_stop()
                except Exception:
                    pass
            time.sleep(delay)
            delay = min(delay * 2, 60)
        finally:
            if process:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                if process.stderr:
                    process.stderr.close()
                if process.stdout:
                    process.stdout.close()


app = FastAPI(title="OCR-To-HA", version="1.0")


@app.get("/static/app.js")
def app_script(request: Request):
    base_path = request.headers.get("x-ingress-path", "").rstrip("/")
    if base_path and not re.fullmatch(r"/[A-Za-z0-9/_-]+", base_path):
        base_path = ""
    script = Path("static/app.js").read_text()
    script = script.replace("api='/api'", f"api='{base_path}/api'")
    script = script.replace('src="/api/feeds/', f'src="{base_path}/api/feeds/')
    return Response(script, media_type="application/javascript")


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
async def startup():
    with lock:
        for feed in config["feeds"]:
            states.setdefault(feed["id"], {"status": "waiting", "results": {}})
            threading.Thread(target=feed_worker, args=(feed["id"],), daemon=True).start()


@app.on_event("shutdown")
def shutdown():
    stop_event.set()


@app.middleware("http")
async def ingress_access(request: Request, call_next):
    if os.getenv("OCR_TO_HA_INGRESS") == "1" and (request.client is None or request.client.host != "172.30.32.2"):
        return JSONResponse(status_code=403, content={"detail": "Requests must come through Home Assistant ingress"})
    return await call_next(request)


@app.get("/")
def index(request: Request):
    base_path = request.headers.get("x-ingress-path", "").rstrip("/")
    if base_path and not re.fullmatch(r"/[A-Za-z0-9/_-]+", base_path):
        base_path = ""
    page = Path("static/index.html").read_text()
    page = page.replace('src="/static/', f'src="{base_path}/static/')
    return HTMLResponse(page)


@app.get("/api/config")
def get_config():
    with lock:
        return {"mqtt": public_mqtt(), "feeds": [{**public_feed(f), "status": public_status(f["id"])} for f in config["feeds"]]}


@app.put("/api/mqtt")
def put_mqtt(value: MQTTInput):
    with lock:
        saved_settings = dict(config["mqtt"])
        incoming = value.model_dump()
        if not incoming["password"]:
            incoming["password"] = saved_settings.get("password", "")
        if homeassistant_mqtt_service():
            for key in ("host", "port", "username", "password", "tls"):
                incoming[key] = saved_settings.get(key, incoming[key])
        config["mqtt"] = incoming
        save_config()
    return public_mqtt()


@app.post("/api/mqtt/test")
def test_mqtt(value: MQTTInput):
    previous = config["mqtt"]
    incoming = value.model_dump()
    if not incoming["password"]:
        incoming["password"] = previous.get("password", "")
    config["mqtt"] = incoming
    try:
        client = mqtt_client()
        client.disconnect()
        client.loop_stop()
        return {"ok": True, "message": "Connected to the MQTT broker"}
    except Exception as exc:
        return JSONResponse(status_code=400, content={"ok": False, "message": str(exc)[:240]})
    finally:
        config["mqtt"] = previous


@app.post("/api/feeds")
def add_feed(value: FeedInput):
    with lock:
        feed = value.model_dump()
        feed["id"] = uuid.uuid4().hex
        feed["url"] = compose_rtsp_url(feed)
        feed.pop("rtsp_password", None)
        feed.pop("clear_rtsp_password", None)
        feed["regions"] = validate_regions(feed.get("regions", []))
        config["feeds"].append(feed)
        save_config()
        states[feed["id"]] = {"status": "waiting", "results": {}}
    threading.Thread(target=feed_worker, args=(feed["id"],), daemon=True).start()
    return {**public_feed(feed), "status": public_status(feed["id"])}


def validate_regions(regions):
    out = []
    for region in regions:
        name = str(region.get("name", "")).strip()
        if not name or len(name) > 80 or any(c in name for c in "+#\x00"):
            raise HTTPException(422, "Region names must be 1–80 characters and cannot contain MQTT wildcards")
        if region.get("rule", "trim") not in ("trim", "numeric"):
            raise HTTPException(422, "Rule must be trim or numeric")
        if region.get("ocr_mode", "auto") not in ("auto", "text", "numeric"):
            raise HTTPException(422, "OCR mode must be auto, text, or numeric")
        if region.get("preprocess", "enhance") not in ("original", "contrast", "sharpen", "threshold", "enhance"):
            raise HTTPException(422, "Unknown OCR preprocessing mode")
        expected_format = str(region.get("expected_format", "")).strip()
        try:
            normalize_expected_pattern(expected_format)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        try:
            upscale = int(region.get("upscale", 3))
            min_confidence = float(region.get("min_confidence", 0))
        except (TypeError, ValueError):
            raise HTTPException(422, "OCR scale and minimum confidence must be numbers")
        if upscale < 1 or upscale > 5:
            raise HTTPException(422, "OCR upscale must be between 1 and 5")
        if not math.isfinite(min_confidence) or min_confidence < 0 or min_confidence > 100:
            raise HTTPException(422, "Minimum OCR confidence must be between 0 and 100")
        clean = {k: region.get(k) for k in ("x", "y", "width", "height", "frame_width", "frame_height")}
        if any(not isinstance(v, (int, float)) for v in clean.values()) or clean["width"] <= 0 or clean["height"] <= 0:
            raise HTTPException(422, "Regions need valid positive coordinates and dimensions")
        bounds = {}
        for key in ("min_value", "max_value"):
            bound = region.get(key)
            if bound in (None, ""):
                bounds[key] = None
            elif isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound):
                raise HTTPException(422, f"{key.replace('_', ' ').capitalize()} must be a finite number")
            else:
                bounds[key] = float(bound)
        if bounds["min_value"] is not None and bounds["max_value"] is not None and bounds["min_value"] > bounds["max_value"]:
            raise HTTPException(422, "Minimum accepted value cannot exceed maximum accepted value")
        out.append({**clean, **bounds, "id": region.get("id") or uuid.uuid4().hex, "name": name, "unit": str(region.get("unit", ""))[:32], "rule": region.get("rule", "trim"), "ocr_mode": region.get("ocr_mode", "auto"), "preprocess": region.get("preprocess", "enhance"), "upscale": upscale, "min_confidence": min_confidence, "expected_format": expected_format})
    return out


@app.put("/api/feeds/{feed_id}")
def edit_feed(feed_id: str, value: FeedInput):
    with lock:
        feed = next((f for f in config["feeds"] if f["id"] == feed_id), None)
        if not feed:
            raise HTTPException(404, "Feed not found")
        updated = value.model_dump()
        updated["url"] = compose_rtsp_url(updated, feed.get("url", ""))
        updated.pop("rtsp_password", None)
        updated.pop("clear_rtsp_password", None)
        removed_regions = {r["id"] for r in feed.get("regions", [])} - {r.get("id") for r in updated.get("regions", [])}
        updated["id"] = feed_id
        updated["regions"] = validate_regions(updated.get("regions", []))
        feed.clear()
        feed.update(updated)
        save_config()
    if removed_regions:
        clear_discovery(feed_id, removed_regions)
    return {**public_feed(feed), "status": public_status(feed_id)}


@app.delete("/api/feeds/{feed_id}")
def delete_feed(feed_id: str):
    with lock:
        feed = next((f for f in config["feeds"] if f["id"] == feed_id), None)
        if not feed:
            raise HTTPException(404, "Feed not found")
        try:
            client = mqtt_client(timeout=2)
            prefix = config["mqtt"]["topic_prefix"]
            device_id = "ocr_to_ha_" + feed_id
            client.publish(f"{prefix}/ocr_to_ha/{feed_id}/availability", "offline", retain=True)
            for region in feed.get("regions", []):
                client.publish(f"{prefix}/sensor/{device_id}_{region['id']}/config", "", retain=True)
            client.disconnect()
            client.loop_stop()
        except Exception:
            pass
        config["feeds"].remove(feed)
        save_config()
        states.pop(feed_id, None)
        frames.pop(feed_id, None)
        raw_frames.pop(feed_id, None)
    return {"ok": True}


@app.get("/api/feeds/{feed_id}/preview")
def preview(feed_id: str, rotation: int | None = None):
    feed = next((item for item in config["feeds"] if item["id"] == feed_id), None)
    image = raw_frames.get(feed_id)
    if image is None:
        raise HTTPException(404, "No frame captured yet; check RTSP settings and feed status")
    current_rotation = feed.get("rotation", 0) if feed else 0
    if rotation is not None:
        if rotation < 0 or rotation > 359:
            raise HTTPException(422, "Rotation must be between 0 and 359 degrees")
        current_rotation = rotation
    image = rotate(image, current_rotation)
    return {"image": encode_preview(image), "width": image.shape[1], "height": image.shape[0], "rotation": rotation if rotation is not None else current_rotation, "timestamp": public_status(feed_id).get("last_capture")}


@app.post("/api/feeds/{feed_id}/preview")
def refresh_preview(feed_id: str):
    status = public_status(feed_id)
    if not status:
        raise HTTPException(404, "Feed not found")
    refresh_events.setdefault(feed_id, threading.Event()).set()
    return {"ok": True, "message": "Preview refresh queued"}


@app.post("/api/feeds/{feed_id}/regions/{region_id}/test")
def test_region(feed_id: str, region_id: str):
    with lock:
        feed = next((f for f in config["feeds"] if f["id"] == feed_id), None)
        region = next((r for r in feed.get("regions", []) if r["id"] == region_id), None) if feed else None
        image = frames.get(feed_id)
    if not feed or not region:
        raise HTTPException(404, "Feed or region not found")
    if image is None:
        raise HTTPException(409, "No frame available yet")
    try:
        return ocr_region(image, region)
    except Exception as exc:
        raise HTTPException(422, str(exc)[:200])


@app.get("/api/health")
def health():
    return {"ok": True, "feeds": len(config["feeds"])}
