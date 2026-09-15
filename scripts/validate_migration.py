"""Reconcile the frozen Parquet baseline with Iceberg; run with streaming stopped."""

from datetime import datetime, timezone
import json
from pathlib import Path

from trino_client import query


def main():
    result = {"timestamp": datetime.now(timezone.utc).isoformat()}
    result["baseline_missing_in_iceberg"] = query("""
        SELECT count(*) n FROM tfm_legacy.bronze.matter_events old
        LEFT JOIN tfm.iot_bronze.matter_events new
        ON old.kafka_topic = new.kafka_topic AND old.kafka_partition = new.kafka_partition
           AND old.kafka_offset = new.kafka_offset
        WHERE new.observation_id IS NULL
    """)[0]["n"]
    result["raw_payload_mismatches"] = query("""
        SELECT count(*) n FROM tfm_legacy.bronze.matter_events old
        JOIN tfm.iot_bronze.matter_events new
        ON old.kafka_topic = new.kafka_topic AND old.kafka_partition = new.kafka_partition
           AND old.kafka_offset = new.kafka_offset
        WHERE old.raw_value IS DISTINCT FROM new.raw_value
    """)[0]["n"]
    result["counts"] = {r["layer"]: r["n"] for r in query("""
        SELECT 'bronze' layer, count(*) n FROM tfm.iot_bronze.matter_events
        UNION ALL SELECT 'silver', count(*) FROM tfm.iot_silver.matter_events
        UNION ALL SELECT 'quarantine', count(*) FROM tfm.iot_quality.quarantine_events
        UNION ALL SELECT 'gold_rows', count(*) FROM tfm.iot_gold.entity_5m
        UNION ALL SELECT 'gold_events', coalesce(sum(event_count),0) FROM tfm.iot_gold.entity_5m
    """)}
    result["silver_duplicate_ids"] = query("SELECT count(*) - count(DISTINCT event_id) n FROM tfm.iot_silver.matter_events")[0]["n"]
    result["bronze_duplicate_observations"] = query("SELECT count(*) - count(DISTINCT observation_id) n FROM tfm.iot_bronze.matter_events")[0]["n"]
    result["quarantine_reasons"] = query("SELECT quality_errors, count(*) n FROM tfm.iot_quality.quarantine_events GROUP BY 1")
    result["quarantine_without_reason"] = query("SELECT count(*) n FROM tfm.iot_quality.quarantine_events WHERE cardinality(quality_errors)=0 OR quality_errors IS NULL")[0]["n"]
    result["snapshots"] = {}
    for schema, table in [("iot_bronze", "matter_events"), ("iot_silver", "matter_events"),
                          ("iot_gold", "entity_5m"), ("iot_quality", "quarantine_events")]:
        result["snapshots"][schema] = query(f'SELECT committed_at, snapshot_id, operation FROM tfm.{schema}."{table}$snapshots" ORDER BY committed_at')
    for key in ["baseline_missing_in_iceberg", "raw_payload_mismatches", "silver_duplicate_ids", "bronze_duplicate_observations", "quarantine_without_reason"]:
        assert result[key] == 0, (key, result[key])
    assert result["counts"]["silver"] == result["counts"]["gold_events"]
    result["status"] = "passed"
    Path("docs/evidence/migration.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
