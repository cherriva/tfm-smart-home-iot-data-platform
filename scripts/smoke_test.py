"""End-to-end smoke test: publish safe synthetic events and verify all layers.

Uses the existing MQTT TLS credentials inside mqtt-ingestor. --transport kafka
provides an offline alternative. No Home Assistant device commands are sent.
"""

import argparse
import base64
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

from trino_client import query


def publish(payloads, transport):
    # Code is supplied through stdin, never shell interpolation or command arguments.
    code = "payloads = " + repr(payloads) + "\n"
    if transport == "mqtt":
        code += '''
import os, ssl
import paho.mqtt.publish as publish
messages = [dict(topic="tfm/synthetic/events/sensor/week1_smoke", payload=p, qos=1, retain=False) for p in payloads]
publish.multiple(messages, hostname=os.environ["MQTT_HOST"], port=int(os.environ.get("MQTT_PORT", "8883")),
    auth={"username": os.environ["MQTT_USERNAME"], "password": os.environ["MQTT_PASSWORD"]},
    tls=ssl.create_default_context())
'''
    else:
        code += '''
import os
from confluent_kafka import Producer
p = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "enable.idempotence": True, "acks": "all"})
errors = []
for payload in payloads:
    p.produce(os.environ["KAFKA_TOPIC"], key="week1-smoke", value=payload, callback=lambda err, msg: errors.append(str(err)) if err else None)
assert p.flush(30) == 0
assert not errors, errors
'''
    subprocess.run(["docker", "compose", "exec", "-T", "mqtt-ingestor", "python", "-"], input=code,
                   text=True, check=True, timeout=90)


def wait_for(predicate, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(3)
    raise AssertionError("Timeout waiting for lakehouse commits")


def grafana_request(path, body=None):
    config = json.loads(subprocess.check_output(["docker", "compose", "config", "--format", "json"], text=True))
    env = config["services"]["grafana"]["environment"]
    credentials = (env["GF_SECURITY_ADMIN_USER"] + ":" + env["GF_SECURITY_ADMIN_PASSWORD"]).encode()
    request = urllib.request.Request("http://localhost:3000" + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Basic " + base64.b64encode(credentials).decode(), "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def verify_dashboards():
    health = grafana_request("/api/health")
    assert health["database"] == "ok", health
    checked, nonempty = 0, 0
    for path in sorted(Path("grafana/dashboards").glob("*.json")):
        dashboard = json.loads(path.read_text())
        remote = grafana_request("/api/dashboards/uid/" + dashboard["uid"])["dashboard"]
        assert remote["title"] == dashboard["title"]
        for panel in remote["panels"]:
            for target in panel.get("targets", []):
                if not target.get("rawSQL"):
                    continue
                item = copy.deepcopy(target)
                item.update(datasource={"type": "trino-datasource", "uid": "tfm-trino"},
                            intervalMs=60000, maxDataPoints=1000)
                try:
                    data = grafana_request("/api/ds/query", {"from": "now-30d", "to": "now", "queries": [item]})
                except urllib.error.HTTPError as error:
                    detail = error.read().decode(errors="replace")
                    raise AssertionError(f"Grafana query failed: {path.name} panel={panel['id']} {detail}") from error
                result = data.get("results", {}).get(item["refId"], {})
                assert result and not result.get("error"), (path.name, panel["id"], result)
                assert result.get("status", 200) < 400, (path.name, panel["id"], result)
                checked += 1
                nonempty += int(any(frame.get("data", {}).get("values", [[]])[0] for frame in result.get("frames", [])))
        print("Dashboard verificado: " + path.name, flush=True)
    return {"queries_passed": checked, "queries_with_data": nonempty}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["mqtt", "kafka"], default="mqtt")
    parser.add_argument("--restart", action="store_true", help="Restart Spark and resend the same event")
    parser.add_argument("--dashboards-only", action="store_true")
    parser.add_argument("--output", default="docs/evidence/smoke-test.json")
    args = parser.parse_args()
    if args.dashboards_only:
        print(json.dumps(verify_dashboards()))
        return
    run = "week1_" + uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc)
    base = {"source": "synthetic", "entity_id": "sensor." + run, "domain": "sensor", "state": "22.5",
            "event_timestamp": now.isoformat(), "updated_timestamp": now.isoformat(), "ingestion_timestamp": now.isoformat(),
            "attributes": {"device_class": "temperature", "unit_of_measurement": "°C", "synthetic": True,
                           "dataset_id": run, "generated_by": "week1-smoke"}}
    historic = dict(base, event_timestamp=(now - timedelta(days=90)).isoformat(),
                    updated_timestamp=(now - timedelta(days=90)).isoformat())
    records = [base, base, historic, dict(base, state="999"), dict(base, event_timestamp=None),
               dict(base, state="unavailable"), dict(base, event_timestamp=(now + timedelta(days=1)).isoformat())]
    payloads = [json.dumps(record) for record in records] + ['{"week1_marker":"' + run + '","broken":']
    predicate = "raw_value LIKE '%" + run + "%'"
    def count(table):
        return query(f"SELECT count(*) n FROM tfm.{table} WHERE {predicate}")[0]["n"]
    def settled(expected_bronze):
        return (count("iot_bronze.matter_events") == expected_bronze and
                count("iot_silver.matter_events") == 2 and count("iot_quality.quarantine_events") == 5 and
                query(f"SELECT coalesce(sum(event_count),0) n FROM tfm.iot_gold.entity_5m WHERE entity_id='sensor.{run}'")[0]["n"] == 2)
    print("Publicando eventos de prueba por " + args.transport, flush=True)
    publish(payloads, args.transport)
    wait_for(lambda: settled(8))
    assert query(f"SELECT quality_warnings FROM tfm.iot_silver.matter_events WHERE {predicate} AND event_time < TIMESTAMP '{(now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')}'")[0]["quality_warnings"] == ["late_event"]
    errors = query(f"SELECT quality_errors FROM tfm.iot_quality.quarantine_events WHERE {predicate}")
    assert all(row["quality_errors"] for row in errors)
    assert query(f"SELECT count(*) n FROM tfm.iot_quality.duplicate_events WHERE {predicate}")[0]["n"] == 1
    ddl = {}
    for table in ["iot_bronze.matter_events", "iot_silver.matter_events", "iot_gold.entity_5m", "iot_quality.quarantine_events"]:
        create = next(iter(query("SHOW CREATE TABLE tfm." + table)[0].values()))
        assert "format_version = 2" in create, create
        snapshots = query(f'SELECT count(*) n FROM tfm.{table.split(".")[0]}."{table.split(".")[1]}$snapshots"')[0]["n"]
        assert snapshots > 0
        ddl[table] = {"ddl": create, "snapshots": snapshots}
    if args.restart:
        print("Reiniciando Spark y reenviando el mismo evento", flush=True)
        subprocess.run(["docker", "compose", "restart", "spark-lakehouse"], check=True, timeout=90)
        publish([json.dumps(base)], args.transport)
        wait_for(lambda: settled(9))
        assert query(f"SELECT count(*) n FROM tfm.iot_quality.duplicate_events WHERE {predicate}")[0]["n"] == 2
    result = {"status": "passed", "run_id": run, "timestamp": datetime.now(timezone.utc).isoformat(),
              "transport": args.transport, "restart_verified": args.restart,
              "bronze_observations": 9 if args.restart else 8, "silver_unique": 2, "quarantined": 5,
              "gold_events": 2, "iceberg": ddl, "grafana": verify_dashboards()}
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": "passed", "evidence": args.output}), flush=True)


if __name__ == "__main__":
    main()
