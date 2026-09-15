"""IoT contract v1. Pure functions shared by Spark and the unit tests."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
import re

UTC = timezone.utc
SCHEMA_VERSION = 1
RULES_VERSION = "quality-v1"


@dataclass(frozen=True)
class QualityConfig:
    temperature_min_c: float = -40.0
    temperature_max_c: float = 85.0
    future_tolerance_seconds: int = 300
    late_threshold_seconds: int = 86400
    inactivity_seconds: int = 86400

    def __post_init__(self):
        if not (math.isfinite(self.temperature_min_c) and math.isfinite(self.temperature_max_c)
                and self.temperature_min_c < self.temperature_max_c):
            raise ValueError("Invalid temperature bounds")
        if min(self.future_tolerance_seconds, self.late_threshold_seconds, self.inactivity_seconds) < 0:
            raise ValueError("Quality windows must be non-negative")

    @classmethod
    def from_env(cls):
        return cls(
            float(os.getenv("QUALITY_TEMPERATURE_MIN_C", "-40")),
            float(os.getenv("QUALITY_TEMPERATURE_MAX_C", "85")),
            int(os.getenv("QUALITY_FUTURE_TOLERANCE_SECONDS", "300")),
            int(os.getenv("QUALITY_LATE_THRESHOLD_SECONDS", "86400")),
            int(os.getenv("QUALITY_INACTIVITY_SECONDS", "86400")),
        )


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def parse_timestamp(value):
    """Require ISO-8601 with a timezone; normalize to UTC microsecond precision."""
    if not isinstance(value, str) or not re.match(r"^\d{4}-\d\d-\d\d[Tt ]\d\d:\d\d:\d\d", value):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00").replace("z", "+00:00"))
        if stamp.tzinfo is None:
            return None
        return stamp.astimezone(UTC)
    except (ValueError, OverflowError):
        return None


def timestamp_key(value):
    stamp = parse_timestamp(value)
    return stamp.isoformat(timespec="microseconds") if stamp else value


def text(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def numeric(value):
    if not isinstance(value, str) or not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def normalize(raw_value, broker_time, config=None):
    """Validate without losing the raw record. Delayed events remain valid warnings.

    broker_time is the stable Kafka timestamp (not wall clock at replay).
    Missing/invalid event times are never replaced by broker_time in Silver.
    """
    config = config or QualityConfig()
    errors, warnings = [], []
    try:
        payload = json.loads(raw_value, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        if not isinstance(payload, dict):
            errors.append("invalid_payload_type")
            payload = {}
    except (ValueError, TypeError):
        errors.append("invalid_json")
        payload = {}

    source, entity, domain = (text(payload.get(k)) for k in ("source", "entity_id", "domain"))
    state_raw = payload.get("state")
    state = text(state_raw)
    if isinstance(state_raw, (int, float)) and not isinstance(state_raw, bool):
        state = str(state_raw)
    attrs = payload.get("attributes", {})
    if not isinstance(attrs, dict):
        errors.append("invalid_attributes")
        attrs = {}

    if source not in {"matter", "homekit", "synthetic", "sonoff"}:
        errors.append("missing_source" if source is None else "invalid_source")
    if not entity:
        errors.append("missing_entity")
    elif not re.fullmatch(r"[a-z_]+\.[a-z0-9_]+", entity):
        errors.append("invalid_entity")
    if not domain:
        errors.append("missing_domain")
    elif entity and entity.split(".")[0] != domain:
        errors.append("domain_mismatch")
    if state is None:
        errors.append("missing_state" if state_raw is None or state_raw == "" else "invalid_state")
    elif state.lower() in {"unknown", "unavailable"}:
        errors.append("state_" + state.lower())
    input_version = payload.get("schema_version", 1)
    if type(input_version) is not int or input_version != SCHEMA_VERSION:
        errors.append("unsupported_schema_version")

    times = {}
    for field, output in [("event_timestamp", "event_time"), ("updated_timestamp", "updated_time"),
                          ("ingestion_timestamp", "ingestion_time"), ("bridge_timestamp", "bridge_time")]:
        value = payload.get(field)
        times[output] = parse_timestamp(value)
        if times[output] is None:
            if field == "event_timestamp":
                errors.append("missing_event_time" if not value else "invalid_event_time")
            elif value is not None:
                errors.append("invalid_" + output)

    if broker_time is not None and broker_time.tzinfo is None:
        broker_time = broker_time.replace(tzinfo=UTC)
    if broker_time is None:
        errors.append("missing_broker_time")
    elif times["event_time"]:
        delay = (broker_time - times["event_time"]).total_seconds()
        if delay < -config.future_tolerance_seconds:
            errors.append("future_event")
        elif delay > config.late_threshold_seconds:
            warnings.append("late_event")

    number = numeric(state)
    device_class = text(attrs.get("device_class"))
    unit = text(attrs.get("unit_of_measurement"))
    if device_class in {"temperature", "humidity"} and state and state.lower() not in {"unknown", "unavailable"}:
        if number is None:
            errors.append("invalid_numeric_state")
        elif device_class == "temperature":
            if unit not in {"°C", "C", "°F", "F", "K"}:
                errors.append("unsupported_temperature_unit")
            else:
                celsius = (number - 32) * 5 / 9 if unit in {"°F", "F"} else number - 273.15 if unit == "K" else number
                if not config.temperature_min_c <= celsius <= config.temperature_max_c:
                    errors.append("temperature_out_of_range")
        elif unit != "%":
            errors.append("unsupported_humidity_unit")
        elif not 0 <= number <= 100:
            errors.append("humidity_out_of_range")
    if state and state.lower() in {"nan", "inf", "infinity", "+inf", "-inf", "-infinity"}:
        errors.append("non_finite_state")
    elif state and re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", state) and number is None:
        errors.append("non_finite_state")

    # Deliberately exclude Kafka, ingestion, bridge and processing metadata.
    identity = [SCHEMA_VERSION, source, entity, domain, state,
                timestamp_key(payload.get("event_timestamp")),
                timestamp_key(payload.get("updated_timestamp")), attrs]
    if "invalid_json" in errors or "invalid_payload_type" in errors:
        identity.append(raw_value)
    attr_map = {str(k): v if isinstance(v, str) else canonical(v) for k, v in attrs.items()}
    return {
        "event_id": digest(identity), "schema_version": SCHEMA_VERSION,
        "source": source, "entity_id": entity, "domain": domain, "state": state,
        "numeric_state": number, "unit": unit, "device_class": device_class,
        "area_id": text(attrs.get("area_id")), "friendly_name": text(attrs.get("friendly_name")),
        "attributes": attr_map, "attributes_json": canonical(attrs),
        "event_timestamp": payload.get("event_timestamp") if isinstance(payload.get("event_timestamp"), str) else None,
        "updated_timestamp": payload.get("updated_timestamp") if isinstance(payload.get("updated_timestamp"), str) else None,
        "ingestion_timestamp": payload.get("ingestion_timestamp") if isinstance(payload.get("ingestion_timestamp"), str) else None,
        "bridge_timestamp": payload.get("bridge_timestamp") if isinstance(payload.get("bridge_timestamp"), str) else None,
        "mqtt_topic": text(payload.get("mqtt_topic")), **times,
        "quality_status": "quarantined" if errors else "warning" if warnings else "valid",
        "quality_flag": errors[0] if errors else warnings[0] if warnings else "ok",
        "quality_errors": list(dict.fromkeys(errors)), "quality_warnings": warnings,
        "quality_rules_version": RULES_VERSION,
        "quality_config": canonical(config.__dict__),
    }
