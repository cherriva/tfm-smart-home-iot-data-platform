"""Reproducible synthetic Matter telemetry generator for the TFM demo."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
import random
import re
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("tfm-synthetic-generator")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} debe ser un número entero") from exc
    return max(minimum, value)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tfm.matter.events")
INVENTORY_PATH = Path(os.getenv("SYNTHETIC_INVENTORY_PATH", "/data/matter_inventory.csv"))
STATE_DIR = Path(os.getenv("SYNTHETIC_STATE_DIR", "/state"))
DATASET_ID = os.getenv("SYNTHETIC_DATASET_ID", "tfm-demo-v1")
ENTITY_PREFIX = os.getenv("SYNTHETIC_ENTITY_PREFIX", "synthetic_")
SEED = env_int("SYNTHETIC_SEED", 20260913)
# Forty days of deterministic history ending at the execution time.
BOOTSTRAP_HOURS = env_int("SYNTHETIC_BOOTSTRAP_HOURS", 24 * 40)
BOOTSTRAP_STEP_SECONDS = env_int("SYNTHETIC_BOOTSTRAP_STEP_SECONDS", 900, 60)
INTERVAL_SECONDS = env_int("SYNTHETIC_INTERVAL_SECONDS", 300, 30)
ENABLED = env_bool("SYNTHETIC_ENABLED", True)
INCLUDE_REGISTRY_ONLY = env_bool("SYNTHETIC_INCLUDE_REGISTRY_ONLY", False)

RUNNING = True


def stop(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def floor_time(value: datetime, step_seconds: int) -> datetime:
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % step_seconds), timezone.utc)


def stable_random(*parts: object) -> random.Random:
    material = "|".join(str(part) for part in (SEED, DATASET_ID, *parts)).encode("utf-8")
    digest = hashlib.blake2b(material, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def as_float(value: str, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_number(value: float, decimals: int = 2) -> str:
    return f"{value:.{decimals}f}".rstrip("0").rstrip(".")


def synthetic_entity_id(original_entity_id: str, domain: str | None = None) -> str:
    if "." in original_entity_id:
        embedded_domain, object_id = original_entity_id.split(".", 1)
    else:
        embedded_domain, object_id = "", original_entity_id

    effective_domain = (domain or embedded_domain).strip()
    if not effective_domain:
        raise ValueError(
            f"No se puede construir el entity_id sintético sin dominio: {original_entity_id}"
        )
    return f"{effective_domain}.{ENTITY_PREFIX}{object_id}"


def stringify_attribute(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def load_inventory() -> list[dict[str, str]]:
    if not INVENTORY_PATH.exists():
        raise FileNotFoundError(f"No existe el inventario: {INVENTORY_PATH}")

    with INVENTORY_PATH.open(newline="", encoding="utf-8") as inventory_file:
        rows = list(csv.DictReader(inventory_file))

    selected = [
        row
        for row in rows
        if row.get("entity_id")
        and (INCLUDE_REGISTRY_ONLY or row.get("entity_status") == "active")
    ]
    # Enrich the real inventory with a deterministic synthetic sensor catalog.
    # Synthetic entities share the same event contract and are published to
    # the same Kafka topic, but are marked source=synthetic in the payload.
    rooms = {
        'dormitorio1': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'hvac'],
        'dormitorio2': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'hvac'],
        'dormitorio3': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'hvac'],
        'bano1': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'extractor', 'water_flow', 'water_leak'],
        'bano2': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'extractor', 'water_flow', 'water_leak'],
        'bano3': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'extractor', 'water_flow', 'water_leak'],
        'salon': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'door', 'window', 'power', 'hvac'],
        'hall': ['temperature', 'humidity', 'presence', 'co2', 'illuminance', 'light', 'window', 'power', 'lock'],
        'garaje': ['temperature', 'humidity', 'presence', 'co2', 'co', 'illuminance', 'light', 'window', 'power', 'vehicle'],
        'exterior': ['temperature', 'humidity', 'irradiance'],
    }
    specs = {
        'temperature': ('sensor', 'temperature', '°C', '23'), 'humidity': ('sensor', 'humidity', '%', '50'),
        'presence': ('binary_sensor', 'occupancy', '', 'off'), 'co2': ('sensor', 'carbon_dioxide', 'ppm', '600'),
        'illuminance': ('sensor', 'illuminance', 'lx', '150'), 'light': ('light', '', '', 'off'),
        'door': ('binary_sensor', 'door', '', 'off'), 'window': ('binary_sensor', 'window', '', 'off'),
        'power': ('sensor', 'power', 'W', '0'), 'hvac': ('climate', '', '°C', 'off'),
        'extractor': ('switch', '', '', 'off'), 'water_flow': ('sensor', 'water', 'L/min', '0'),
        'water_leak': ('binary_sensor', 'moisture', '', 'off'), 'lock': ('lock', '', '', 'locked'),
        'co': ('sensor', 'carbon_monoxide', 'ppm', '0'), 'vehicle': ('binary_sensor', 'presence', '', 'off'),
        'irradiance': ('sensor', 'irradiance', 'W/m²', '0'),
    }
    for room, devices in rooms.items():
        for device in devices:
            domain, device_class, unit, current = specs[device]
            selected.append({'device_id': f'synthetic_{room}_{device}', 'device_name': f'{device} {room}',
                             'area_id': room, 'entity_id': f'{domain}.{device}_{room}',
                             'entity_name': f'{device} {room}', 'domain': domain, 'entity_status': 'active',
                             'current_state': current, 'unit_of_measurement': unit, 'device_class': device_class,
                             'state_class': 'measurement' if domain == 'sensor' else '', 'attributes_json': '{}'})
    selected.sort(key=lambda row: (row.get("domain", ""), row["entity_id"]))
    LOGGER.info(
        "Inventario cargado entities=%s total=%s include_registry_only=%s",
        len(selected),
        len(rows),
        INCLUDE_REGISTRY_ONLY,
    )
    return selected


def base_attributes(row: dict[str, str]) -> dict[str, str]:
    raw_attributes = row.get("attributes_json", "")
    try:
        parsed_attributes = json.loads(raw_attributes) if raw_attributes else {}
    except json.JSONDecodeError:
        parsed_attributes = {}

    attributes = {
        str(key): stringify_attribute(value) for key, value in parsed_attributes.items()
    }
    friendly_name = row.get("entity_name") or row["entity_id"]
    area = row.get("area_id") or "sin_area"
    attributes.update(
        {
            "friendly_name": f"{friendly_name} · {area}",
            "synthetic": "true",
            "dataset_id": DATASET_ID,
            "original_entity_id": row["entity_id"],
            "inventory_device_id": row.get("device_id", ""),
            "device_name": row.get("device_name", ""),
            "manufacturer": row.get("manufacturer", ""),
            "model": row.get("model", ""),
            "area_id": area,
            "generated_by": "tfm-synthetic-generator",
        }
    )
    if row.get("unit_of_measurement"):
        attributes["unit_of_measurement"] = row["unit_of_measurement"]
    if row.get("device_class"):
        attributes["device_class"] = row["device_class"]
    if row.get("state_class"):
        attributes["state_class"] = row["state_class"]
    return attributes


def area_offset(area_id: str) -> float:
    rng = stable_random("area", area_id)
    return rng.uniform(-1.2, 1.2)


def device_load_watts(row: dict[str, str], timestamp: datetime) -> float:
    hint = f"{row.get('device_name', '')} {row.get('entity_id', '')}".lower()
    hour = timestamp.hour + timestamp.minute / 60.0
    rng = stable_random("load", row.get("device_id", ""), iso_timestamp(timestamp))

    if "calefaccion" in hint:
        active = 6.5 <= hour <= 9.0 or 19.0 <= hour <= 23.0
        return rng.uniform(900, 1800) if active and rng.random() < 0.75 else 0.0
    if "persiana" in hint:
        movement = (7.5 <= hour <= 8.5) or (20.0 <= hour <= 21.0)
        return rng.uniform(80, 180) if movement and rng.random() < 0.35 else 0.0
    if "lampara" in hint or "luz" in hint:
        active = hour >= 18.0 or hour <= 1.0
        return rng.uniform(6, 45) if active and rng.random() < 0.72 else 0.0
    return rng.uniform(8, 35)


def binary_state(row: dict[str, str], timestamp: datetime) -> str:
    hint = f"{row.get('device_name', '')} {row.get('entity_id', '')}".lower()
    hour = timestamp.hour + timestamp.minute / 60.0
    rng = stable_random("binary", row["entity_id"], iso_timestamp(timestamp))

    if "persiana" in hint:
        if "subir" in hint:
            return "on" if 7.5 <= hour <= 8.5 and rng.random() < 0.45 else "off"
        if "bajar" in hint:
            return "on" if 20.0 <= hour <= 21.0 and rng.random() < 0.45 else "off"
    if "calefaccion" in hint:
        active = 6.5 <= hour <= 9.0 or 19.0 <= hour <= 23.0
        return "on" if active and rng.random() < 0.72 else "off"
    if "lampara" in hint or "luz" in hint:
        active = hour >= 18.0 or hour <= 1.0
        return "on" if active and rng.random() < 0.7 else "off"
    if row.get("domain") == "binary_sensor":
        return "on" if rng.random() < 0.09 else "off"
    return "on" if rng.random() < 0.3 else "off"


def synthetic_state(row: dict[str, str], timestamp: datetime) -> str:
    domain = row.get("domain", "sensor")
    device_class = row.get("device_class", "")
    unit = row.get("unit_of_measurement", "")
    current_state = row.get("current_state", "")
    rng = stable_random("state", row["entity_id"], iso_timestamp(timestamp))
    hour = timestamp.hour + timestamp.minute / 60.0
    daily_angle = 2 * math.pi * (hour - 15.0) / 24.0

    if domain == "sensor":
        if device_class == "temperature" or unit == "°C":
            base = 23.2 + area_offset(row.get("area_id", ""))
            value = base + 1.4 * math.cos(daily_angle) + rng.uniform(-0.22, 0.22)
            return format_number(value, 2)
        if device_class == "humidity":
            base = 49.0 - area_offset(row.get("area_id", "")) * 2.0
            shower_peak = 5.5 if 6.5 <= hour <= 9.0 or 20.0 <= hour <= 22.5 else 0.0
            value = base - 4.0 * math.cos(daily_angle) + shower_peak + rng.uniform(-0.7, 0.7)
            return format_number(min(75.0, max(30.0, value)), 2)
        if device_class == "battery":
            level = 92.0 + stable_random("battery", row["entity_id"]).uniform(0, 8)
            level -= (timestamp.timetuple().tm_yday % 30) * 0.03
            return format_number(min(100.0, max(20.0, level)), 1)
        if device_class == "voltage" or unit == "V":
            base = as_float(current_state, 3.0)
            return format_number(max(2.4, base + rng.uniform(-0.025, 0.025)), 3)
        if device_class == "power" or unit == "W":
            return format_number(device_load_watts(row, timestamp), 2)
        if device_class == "current" or unit == "A":
            return format_number(device_load_watts(row, timestamp) / 230.0, 3)
        if device_class == "energy" or unit == "kWh":
            base = as_float(current_state, stable_random("energy-base", row["entity_id"]).uniform(0.2, 4.0))
            anchor = datetime(2026, 1, 1, tzinfo=timezone.utc)
            elapsed_hours = max(0.0, (timestamp - anchor).total_seconds() / 3600.0)
            rate = stable_random("energy-rate", row["entity_id"]).uniform(0.0004, 0.0025)
            return format_number(base + elapsed_hours * rate, 4)
        if current_state and current_state not in {"unknown", "unavailable"}:
            if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", current_state):
                return format_number(as_float(current_state, 0.0) + rng.uniform(-0.2, 0.2), 2)
            return current_state
        return "ok"

    if domain in {"light", "switch", "binary_sensor"}:
        return binary_state(row, timestamp)
    if domain == "lock":
        return "unlocked" if rng.random() < 0.08 else "locked"
    if domain in {"event", "button"}:
        return iso_timestamp(timestamp)
    if domain == "update":
        return "on" if rng.random() < 0.01 else "off"
    if domain == "select":
        return current_state if current_state not in {"", "unknown", "unavailable"} else "normal"
    if domain == "number":
        return format_number(as_float(current_state, 10.0), 1)
    return current_state if current_state not in {"", "unknown", "unavailable"} else "ok"


def should_emit(row: dict[str, str], timestamp: datetime, force: bool) -> bool:
    if force:
        return True
    domain = row.get("domain", "")
    rng = stable_random("emit", row["entity_id"], iso_timestamp(timestamp))
    if domain in {"event", "button"}:
        return rng.random() < 0.08
    if domain in {"update", "select", "number"}:
        return timestamp.minute == 0
    return True


def build_event(row: dict[str, str], timestamp: datetime) -> tuple[str, dict[str, Any]]:
    original_entity_id = row["entity_id"]
    entity_id = synthetic_entity_id(original_entity_id, row.get("domain"))
    event_identifier = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{DATASET_ID}|{original_entity_id}|{iso_timestamp(timestamp)}",
        )
    )
    latency_rng = stable_random("latency", original_entity_id, iso_timestamp(timestamp))
    ingestion_timestamp = timestamp + timedelta(milliseconds=latency_rng.randint(45, 850))
    attributes = base_attributes(row)
    attributes["synthetic_id"] = event_identifier

    if row.get("domain") == "event":
        attributes["event_type"] = row.get("event_type") or "initial_press"
        attributes["newPosition"] = str(latency_rng.choice([1, 2]))

    state = synthetic_state(row, timestamp)

    record = {
        "source": "synthetic",
        "entity_id": entity_id,
        "domain": row.get("domain") or entity_id.split(".", 1)[0],
        "state": state,
        "event_timestamp": iso_timestamp(timestamp),
        "ingestion_timestamp": iso_timestamp(ingestion_timestamp),
        "updated_timestamp": iso_timestamp(timestamp),
        "mqtt_topic": f"tfm/synthetic/events/{row.get('domain', 'unknown')}/{entity_id}",
        "bridge_timestamp": iso_timestamp(ingestion_timestamp),
        "attributes": attributes,
    }
    return entity_id, record


class EventPublisher:
    def __init__(self) -> None:
        self.delivery_errors = 0
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "tfm-synthetic-generator",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "snappy",
            }
        )

    def delivery_report(self, error: Any, _message: Any) -> None:
        if error is not None:
            self.delivery_errors += 1
            LOGGER.error("Error entregando evento sintético: %s", error)

    def publish(self, key: str, record: dict[str, Any]) -> None:
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        while RUNNING:
            try:
                self.producer.produce(
                    KAFKA_TOPIC,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=self.delivery_report,
                )
                self.producer.poll(0)
                return
            except BufferError:
                self.producer.poll(1)

    def flush(self) -> None:
        pending = self.producer.flush(30)
        if pending:
            raise RuntimeError(f"Quedaron {pending} eventos sintéticos sin entregar")
        if self.delivery_errors:
            raise RuntimeError(f"Fallaron {self.delivery_errors} entregas sintéticas")


def state_path() -> Path:
    safe_dataset_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", DATASET_ID)
    return STATE_DIR / f"{safe_dataset_id}.json"


def load_state() -> dict[str, Any]:
    path = state_path()
    if not path.exists():
        return {"dataset_id": DATASET_ID, "published_events": 0}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"No se pudo leer el estado sintético {path}: {exc}") from exc
    if state.get("dataset_id") != DATASET_ID:
        return {"dataset_id": DATASET_ID, "published_events": 0}
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = state_path()
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary_path.replace(path)


def publish_cycle(
    publisher: EventPublisher,
    inventory: list[dict[str, str]],
    timestamp: datetime,
    force_all: bool = False,
) -> int:
    published = 0
    for row in inventory:
        if not should_emit(row, timestamp, force_all):
            continue
        key, record = build_event(row, timestamp)
        publisher.publish(key, record)
        published += 1
    publisher.flush()
    return published


def bootstrap(
    publisher: EventPublisher,
    inventory: list[dict[str, str]],
    state: dict[str, Any],
) -> None:
    if state.get("bootstrapped"):
        LOGGER.info(
            "Histórico sintético ya generado dataset_id=%s published_events=%s",
            DATASET_ID,
            state.get("published_events", 0),
        )
        return

    if "bootstrap_anchor" not in state:
        anchor = floor_time(datetime.now(timezone.utc), BOOTSTRAP_STEP_SECONDS)
        start = anchor - timedelta(hours=BOOTSTRAP_HOURS)
        state["bootstrap_anchor"] = iso_timestamp(anchor)
        state["bootstrap_next"] = iso_timestamp(start)
        save_state(state)

    anchor = parse_timestamp(state["bootstrap_anchor"])
    next_timestamp = parse_timestamp(state["bootstrap_next"])
    LOGGER.info(
        "Generando histórico sintético dataset_id=%s from=%s to=%s step_seconds=%s",
        DATASET_ID,
        iso_timestamp(next_timestamp),
        iso_timestamp(anchor),
        BOOTSTRAP_STEP_SECONDS,
    )

    while RUNNING and next_timestamp <= anchor:
        force_all = next_timestamp == anchor
        published = publish_cycle(publisher, inventory, next_timestamp, force_all=force_all)
        state["published_events"] = int(state.get("published_events", 0)) + published
        next_timestamp += timedelta(seconds=BOOTSTRAP_STEP_SECONDS)
        state["bootstrap_next"] = iso_timestamp(next_timestamp)
        state["last_cycle"] = iso_timestamp(next_timestamp - timedelta(seconds=BOOTSTRAP_STEP_SECONDS))
        save_state(state)

    if RUNNING:
        state["bootstrapped"] = True
        state["bootstrapped_at"] = iso_timestamp(datetime.now(timezone.utc))
        save_state(state)
        LOGGER.info(
            "Histórico sintético completado dataset_id=%s published_events=%s",
            DATASET_ID,
            state["published_events"],
        )


def run_forever(
    publisher: EventPublisher,
    inventory: list[dict[str, str]],
    state: dict[str, Any],
) -> None:
    while RUNNING:
        now = datetime.now(timezone.utc)
        last_cycle = parse_timestamp(state["last_cycle"]) if state.get("last_cycle") else None
        next_timestamp = (
            last_cycle + timedelta(seconds=INTERVAL_SECONDS)
            if last_cycle
            else floor_time(now, INTERVAL_SECONDS)
        )

        if now - next_timestamp > timedelta(seconds=INTERVAL_SECONDS * 3):
            next_timestamp = floor_time(now, INTERVAL_SECONDS)

        if next_timestamp <= now:
            published = publish_cycle(publisher, inventory, next_timestamp)
            state["published_events"] = int(state.get("published_events", 0)) + published
            state["last_cycle"] = iso_timestamp(next_timestamp)
            state["last_published_at"] = iso_timestamp(datetime.now(timezone.utc))
            save_state(state)
            LOGGER.info(
                "Ciclo sintético publicado timestamp=%s events=%s total=%s",
                iso_timestamp(next_timestamp),
                published,
                state["published_events"],
            )
            continue

        wait_seconds = min(15.0, max(0.5, (next_timestamp - now).total_seconds()))
        time.sleep(wait_seconds)


def main() -> None:
    if not ENABLED:
        LOGGER.info("Generación sintética desactivada mediante SYNTHETIC_ENABLED=false")
        while RUNNING:
            time.sleep(60)
        return

    inventory = load_inventory()
    if not inventory:
        raise RuntimeError("No hay entidades disponibles para generar datos sintéticos")

    state = load_state()
    publisher = EventPublisher()
    LOGGER.info(
        "Conectando a Redpanda brokers=%s topic=%s dataset_id=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        DATASET_ID,
    )
    bootstrap(publisher, inventory, state)
    if RUNNING:
        run_forever(publisher, inventory, state)
    publisher.flush()
    LOGGER.info("Generador sintético detenido")


if __name__ == "__main__":
    main()
