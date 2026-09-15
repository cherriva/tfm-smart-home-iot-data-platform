"""Integration checks on disposable Iceberg tables using the real Hive/MinIO."""

from datetime import datetime, timedelta, timezone
import json
import uuid

import lakehouse as lake
from event_contract import QualityConfig


def main():
    spark = lake.session("TFM-Week1-Integration")
    namespace = "week1_test_" + uuid.uuid4().hex[:12]
    spark.sql(f"CREATE NAMESPACE lake.{namespace} LOCATION 's3a://tfm-lakehouse/tests/{namespace}'")
    for name, table in [("BRONZE", "bronze"), ("SILVER", "silver"), ("QUARANTINE", "quarantine"),
                        ("GOLD", "gold"), ("INVENTORY", "inventory")]:
        setattr(lake, name, f"lake.{namespace}.{table}")
    lake.initialize(spark)
    config = QualityConfig()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    old = now.replace(hour=0, minute=0, second=0) - timedelta(days=90)
    base = {"source": "synthetic", "entity_id": "sensor.week1_test", "domain": "sensor", "state": "20",
            "event_timestamp": old.isoformat(), "updated_timestamp": old.isoformat(),
            "attributes": {"device_class": "temperature", "unit_of_measurement": "°C"}}
    def frame(records):
        rows = [("test", json.dumps(payload) if not isinstance(payload, str) else payload, "week1-test", 0,
                 offset, now, now) for offset, payload in records]
        return spark.createDataFrame(rows, "entity_key string, raw_value string, kafka_topic string, kafka_partition int, kafka_offset long, kafka_timestamp timestamp, bronze_timestamp timestamp")
    def count(table):
        return spark.table(table).count()
    def verify_counts(b, s, q, events):
        actual = [count(lake.BRONZE), count(lake.SILVER), count(lake.QUARANTINE),
                  spark.sql(f"SELECT sum(event_count) n FROM {lake.GOLD}").first().n]
        assert actual == [b, s, q, events], actual

    checks = []
    batch = frame([(0, base), (1, dict(base, ingestion_timestamp=now.isoformat()))])
    lake.process_batch(spark, batch, 0, config)
    verify_counts(2, 1, 0, 1)
    checks.append("same_batch_duplicates")
    lake.process_batch(spark, batch, 0, config)
    verify_counts(2, 1, 0, 1)
    checks.append("replay_same_offsets")
    lake.process_batch(spark, frame([(2, base)]), 1, config)
    verify_counts(3, 1, 0, 1)
    checks.append("duplicate_across_batches")
    late = dict(base, state="24", event_timestamp=(old + timedelta(seconds=30)).isoformat(),
                updated_timestamp=(old + timedelta(seconds=30)).isoformat())
    lake.process_batch(spark, frame([(3, late)]), 2, config)
    verify_counts(4, 2, 0, 2)
    assert spark.table(lake.GOLD).first().avg_numeric_state == 22.0
    assert all(r.quality_warnings == ["late_event"] for r in spark.table(lake.SILVER).collect())
    checks.append("historical_event_updates_original_window")
    invalid = frame([(4, dict(base, state="200")), (5, '{"broken":'),
                     (6, dict(base, event_timestamp=(now + timedelta(days=1)).isoformat())),
                     (7, dict(base, state=None, event_timestamp=None, entity_id=None))])
    lake.process_batch(spark, invalid, 3, config)
    lake.process_batch(spark, invalid, 3, config)
    verify_counts(8, 2, 4, 2)
    assert all(r.quality_errors for r in spark.table(lake.QUARANTINE).collect())
    checks.append("quarantine_with_explicit_causes_and_replay")

    # Simulate crash after the Silver commit, before Gold commit.
    actuator = dict(base, entity_id="light.week1_test", domain="light", state="on", attributes={})
    recovery = frame([(8, actuator)])
    original = lake.refresh_gold
    def fail(*args):
        raise RuntimeError("injected failure after Silver")
    lake.refresh_gold = fail
    try:
        lake.process_batch(spark, recovery, 4, config)
        raise AssertionError("Expected injected failure")
    except RuntimeError as error:
        assert str(error) == "injected failure after Silver"
    finally:
        lake.refresh_gold = original
    verify_counts(9, 3, 4, 2)
    lake.process_batch(spark, recovery, 4, config)
    verify_counts(9, 3, 4, 3)
    lake.process_batch(spark, recovery, 4, config)
    verify_counts(9, 3, 4, 3)
    checks.append("recovery_after_partial_commit_and_null_unit_merge")

    for table in [lake.BRONZE, lake.SILVER, lake.QUARANTINE, lake.GOLD, lake.INVENTORY]:
        spark.sql(f"DROP TABLE {table} PURGE")
    spark.sql(f"DROP NAMESPACE lake.{namespace}")
    print("INTEGRATION_RESULT=" + json.dumps({"status": "passed", "checks": checks}), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
