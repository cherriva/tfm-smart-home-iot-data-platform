"""Capture versions, counts and sanitized examples; never dump environment."""

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess

from trino_client import query


def command(*args):
    return subprocess.check_output(args, text=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="tfm_legacy")
    parser.add_argument("--output", default="docs/evidence/baseline.json")
    args = parser.parse_args()
    if not args.catalog.replace("_", "").isalnum():
        parser.error("Invalid catalog")
    services = json.loads(command("docker", "compose", "config", "--format", "json"))["services"]
    result = {"captured_at": dt.datetime.now(dt.timezone.utc).isoformat(), "images": {}, "tables": {}}
    for service, config in services.items():
        ids = command("docker", "compose", "ps", "-a", "-q", service).split()
        containers = json.loads(command("docker", "inspect", *ids)) if ids else []
        result["images"][service] = {
            "declared_image": config.get("image"),
            "containers": [{"image": c["Config"]["Image"], "image_id": c["Image"],
                            "status": c["State"]["Status"]} for c in containers],
        }
    for layer, name in [("bronze", "matter_events"), ("silver", "matter_events"), ("gold", "entity_5m")]:
        table = f"{args.catalog}.{layer}.{name}"
        query(f"CALL {args.catalog}.system.sync_partition_metadata('{layer}', '{name}', 'FULL')")
        count = query(f"SELECT count(*) n, count(DISTINCT entity_id) entities FROM {table}")[0]
        fields = "domain, state, event_timestamp" if layer == "bronze" else (
            "domain, state, numeric_state, quality_flag, event_time" if layer == "silver" else
            "domain, window_start, event_count, avg_numeric_state")
        samples = query(f"SELECT {fields} FROM {table} ORDER BY entity_id LIMIT 3")
        count["sanitized_examples"] = samples
        count["ddl"] = query(f"SHOW CREATE TABLE {table}")
        if layer == "gold":
            count["represented_events"] = query(f"SELECT sum(event_count) n FROM {table}")[0]["n"]
        result["tables"][table] = count
    result["source_hashes"] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in [Path("docker-compose.yml"), *Path("spark-bronze").glob("*.py"), Path("trino/init.sql")]}
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": args.output, "counts": {k: v["n"] for k, v in result["tables"].items()}}))


if __name__ == "__main__":
    main()
