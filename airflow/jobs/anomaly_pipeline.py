"""Reproducible rule + Isolation Forest anomaly experiment over Iceberg Silver.

The job only scores labelled synthetic telemetry by default.  It never writes back to
the streaming layers; its sole output is the replay-safe Gold anomaly fact table.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import requests
from sklearn.ensemble import IsolationForest

TRINO = "http://trino:8080/v1/statement"
SEED = int(os.getenv("ANOMALY_MODEL_SEED", "20260914"))
CONTAMINATION = float(os.getenv("ANOMALY_CONTAMINATION", "0.05"))
DAYS = int(os.getenv("ANOMALY_TRAINING_DAYS", "30"))


def sql(statement: str):
    response = requests.post(TRINO, headers={"X-Trino-User": "airflow", "Content-Type": "text/plain"}, data=statement, timeout=120)
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data", [])
    while payload.get("nextUri"):
        response = requests.get(payload["nextUri"], headers={"X-Trino-User": "airflow"}, timeout=120)
        response.raise_for_status()
        payload = response.json()
        rows.extend(payload.get("data", []))
    if payload.get("error"):
        raise RuntimeError(payload["error"].get("message", "Trino error"))
    return rows


def rule(value: float, unit: str | None, delta: float, hours_since_previous: float, hour: int, entity_id: str):
    """Return (type, severity, explanation), deliberately simple and auditable."""
    unit = unit or ""
    name = entity_id.lower()
    if unit == "°C" and not -5 <= value <= 45:
        return "temperature_out_of_range", "high", f"Temperatura {value:.1f} °C fuera del rango físico [-5, 45]."
    if unit == "%" and not 0 <= value <= 100:
        return "humidity_out_of_range", "high", f"Humedad {value:.1f} % fuera del rango [0, 100]."
    if abs(delta) > (8 if unit == "°C" else 25 if unit == "%" else 1000):
        return "abrupt_change", "medium", f"Cambio de {delta:.2f} en una observación consecutiva."
    if hours_since_previous > 24:
        return "sensor_inactive", "medium", f"Han transcurrido {hours_since_previous:.1f} h desde la observación anterior."
    if ("luz" in name or "lampara" in name) and 2 <= hour <= 5 and value > 0:
        return "unusual_light_schedule", "low", "Actividad de iluminación entre las 02:00 y las 05:59."
    return None, None, None


def esc(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (float, int)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def run():
    rows = sql(f"""SELECT event_id, source, entity_id, event_time, numeric_state, unit,
        coalesce(attributes['is_anomaly_expected'], 'false'), attributes['expected_anomaly_type']
      FROM tfm.iot_silver.matter_events
      WHERE source = 'synthetic' AND numeric_state IS NOT NULL
        AND event_time >= date_add('day', -{DAYS}, current_timestamp)
      ORDER BY event_time, entity_id""")
    if len(rows) < 20:
        raise RuntimeError("Se necesitan al menos 20 eventos numéricos sintéticos recientes para entrenar anomalías")

    prepared, previous = [], {}
    for event_id, source, entity_id, event_time, value, unit, expected, expected_type in rows:
        timestamp = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
        value = float(value)
        old = previous.get(entity_id)
        delta = value - old[0] if old else 0.0
        gap = (timestamp - old[1]).total_seconds() / 3600 if old else 0.0
        previous[entity_id] = (value, timestamp)
        hour = timestamp.hour
        prepared.append(dict(event_id=event_id, source=source, entity_id=entity_id, time=timestamp,
            value=value, unit=unit, expected=str(expected).lower() == "true", expected_type=expected_type,
            delta=delta, gap=gap, hour=hour))
    # Strict temporal split: the model learns only from the first 70% of ordered data.
    split = max(10, int(len(prepared) * .7))
    features = np.array([[x["value"], x["delta"], x["gap"], np.sin(x["hour"] * np.pi / 12), np.cos(x["hour"] * np.pi / 12)] for x in prepared])
    model = IsolationForest(n_estimators=200, contamination=CONTAMINATION, random_state=SEED, n_jobs=1)
    model.fit(features[:split])
    scores = -model.score_samples(features)
    threshold = float(np.quantile(scores[:split], 1 - CONTAMINATION))
    model_version = "iforest-" + hashlib.sha256(f"{SEED}|{CONTAMINATION}|{len(rows)}|{prepared[-1]['time'].isoformat()}".encode()).hexdigest()[:12]
    dataset_version = "synthetic-" + hashlib.sha256("|".join(str(x["event_id"]) for x in prepared).encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    output = []
    confusion = defaultdict(int)
    for item, score in zip(prepared, scores):
        r_type, severity, explanation = rule(item["value"], item["unit"], item["delta"], item["gap"], item["hour"], item["entity_id"])
        rule_hit, model_hit = r_type is not None, bool(score >= threshold)
        combined = rule_hit or model_hit
        detected_type = r_type or ("statistical_outlier" if model_hit else "normal")
        if model_hit and not severity: severity, explanation = "medium", f"Isolation Forest score {score:.4f} supera el umbral {threshold:.4f}."
        if not severity: severity, explanation = "none", "Sin regla ni desviación estadística suficiente."
        confusion[(item["expected"], combined)] += 1
        output.append((item, detected_type, "combined" if combined else "none", severity, float(score), threshold, explanation, rule_hit, model_hit, combined))
    # Rebuild only the reproducible synthetic experiment window, preserving real future use.
    sql("DELETE FROM tfm.iot_gold.anomaly_events WHERE source = 'synthetic' AND event_time >= date_add('day', -%s, current_timestamp)" % DAYS)
    columns = "event_id,source,entity_id,event_time,anomaly_type,detection_method,severity,anomaly_score,threshold,explanation,model_version,dataset_version,is_anomaly_expected,expected_anomaly_type,rule_detected,model_detected,combined_detected,feature_value,feature_delta,hour_of_day,scored_at"
    for start in range(0, len(output), 200):
        values = []
        for item, typ, method, severity, score, threshold, explanation, rh, mh, combined in output[start:start + 200]:
            values.append("(" + ",".join([esc(item["event_id"]), esc(item["source"]), esc(item["entity_id"]), esc(item["time"].isoformat()), esc(typ), esc(method), esc(severity), esc(score), esc(threshold), esc(explanation), esc(model_version), esc(dataset_version), esc(item["expected"]), esc(item["expected_type"]), esc(rh), esc(mh), esc(combined), esc(item["value"]), esc(item["delta"]), esc(item["hour"]), esc(now)]) + ")")
        sql("INSERT INTO tfm.iot_gold.anomaly_events (" + columns + ") VALUES " + ",".join(values))
    tp, fp, fn = confusion[(True, True)], confusion[(False, True)], confusion[(True, False)]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    result = {"rows": len(output), "model_version": model_version, "dataset_version": dataset_version,
              "precision": precision, "recall": recall, "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
              "confusion": {f"expected_{a}_detected_{b}": n for (a, b), n in confusion.items()}}
    print(json.dumps(result, sort_keys=True), flush=True)
    return result
