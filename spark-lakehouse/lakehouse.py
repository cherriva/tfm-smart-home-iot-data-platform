"""Shared Iceberg catalog, schemas and replay-safe writes (one active writer)."""

import os
from pathlib import Path
import csv
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, MapType, StringType,
    StructField, StructType, TimestampType,
)

from event_contract import QualityConfig, normalize

BRONZE = "lake.iot_bronze.matter_events"
SILVER = "lake.iot_silver.matter_events"
QUARANTINE = "lake.iot_quality.quarantine_events"
GOLD = "lake.iot_gold.entity_5m"
INVENTORY = "lake.iot_quality.sensor_inventory"
AEMET_DAILY = "lake.iot_silver.aemet_daily"
DATADIS_CONSUMPTION = "lake.iot_silver.datadis_consumption_hourly"
ANOMALY_EVENTS = "lake.iot_gold.anomaly_events"

STRING_FIELDS = """event_id source entity_id domain state unit device_class area_id friendly_name
attributes_json event_timestamp updated_timestamp ingestion_timestamp bridge_timestamp mqtt_topic
quality_status quality_flag quality_rules_version quality_config""".split()
NORMALIZED_SCHEMA = StructType(
    [StructField(k, StringType()) for k in STRING_FIELDS]
    + [StructField("schema_version", IntegerType()), StructField("numeric_state", DoubleType()),
       StructField("attributes", MapType(StringType(), StringType())),
       StructField("quality_errors", ArrayType(StringType())),
       StructField("quality_warnings", ArrayType(StringType()))]
    + [StructField(k, TimestampType()) for k in ["event_time", "updated_time", "ingestion_time", "bridge_time"]]
)


def session(name="TFM-Iceberg"):
    builder = (SparkSession.builder.appName(name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hive")
        .config("spark.sql.catalog.lake.uri", os.getenv("HIVE_METASTORE_URI", "thrift://iceberg-metastore:9083"))
        .config("spark.sql.catalog.lake.warehouse", "s3a://tfm-lakehouse/warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", "http://" + os.getenv("MINIO_ENDPOINT", "minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def initialize(spark):
    for schema in ["iot_bronze", "iot_silver", "iot_gold", "iot_quality"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{schema} LOCATION 's3a://tfm-lakehouse/{schema}'")
    fields = [f"{field.name} {field.dataType.simpleString()}" for field in NORMALIZED_SCHEMA]
    fields += ["observation_id string", "entity_key string", "raw_value string", "kafka_topic string",
               "kafka_partition int", "kafka_offset bigint", "kafka_timestamp timestamp",
               "bronze_timestamp timestamp", "processing_time timestamp", "event_date date"]
    for table in [BRONZE, SILVER, QUARANTINE]:
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} ({','.join(fields)}) USING iceberg
            PARTITIONED BY (days(kafka_timestamp))
            TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
             'write.parquet.compression-codec'='zstd', 'write.distribution-mode'='hash')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD} (
        source string, entity_id string, domain string, unit string,
        window_start timestamp, window_end timestamp,
        event_count bigint, valid_event_count bigint, numeric_event_count bigint,
        avg_numeric_state double, min_numeric_state double, max_numeric_state double,
        last_event_time timestamp, event_date date
    ) USING iceberg PARTITIONED BY (days(window_start)) TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {INVENTORY} (
        source string, entity_id string, domain string, area_id string,
        inactivity_seconds bigint
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {AEMET_DAILY} (
        source string, station_id string, station_name string, province string,
        altitude_m double, observation_date date, mean_temperature_c double,
        min_temperature_c double, max_temperature_c double, precipitation_mm double,
        mean_humidity_pct double, min_humidity_pct double, max_humidity_pct double,
        mean_wind_speed_kmh double, max_gust_kmh double, sunshine_hours double,
        loaded_at timestamp
    ) USING iceberg PARTITIONED BY (months(observation_date))
      TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
                     'write.parquet.compression-codec'='zstd')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DATADIS_CONSUMPTION} (
        cups string, distributor_code string, reading_date date, period string,
        consumption_kwh double, obtain_method string, surplus_energy_kwh double,
        generation_energy_kwh double, self_consumption_energy_kwh double,
        loaded_at timestamp
    ) USING iceberg PARTITIONED BY (months(reading_date))
      TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
                     'write.parquet.compression-codec'='zstd')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ANOMALY_EVENTS} (
        event_id string, source string, entity_id string, event_time timestamp,
        anomaly_type string, detection_method string, severity string,
        anomaly_score double, threshold double, explanation string,
        model_version string, dataset_version string, is_anomaly_expected boolean,
        expected_anomaly_type string, rule_detected boolean, model_detected boolean,
        combined_detected boolean, feature_value double, feature_delta double,
        hour_of_day integer, scored_at timestamp
    ) USING iceberg PARTITIONED BY (days(event_time))
      TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet',
                     'write.parquet.compression-codec'='zstd')""")


def normalize_frame(frame, config):
    normalizer = F.udf(lambda raw, broker: normalize(raw, broker, config), NORMALIZED_SCHEMA)
    normalized = (frame.withColumn("parsed", normalizer("raw_value", "kafka_timestamp"))
                  .select("entity_key", "raw_value", "kafka_topic", "kafka_partition", "kafka_offset",
                          "kafka_timestamp", "bronze_timestamp", "parsed.*")
                  .withColumn("processing_time", F.current_timestamp())
                  .withColumn("event_date", F.to_date("event_time")))
    return normalized.withColumn("observation_id", F.sha2(F.to_json(F.struct(
        F.lit(os.getenv("KAFKA_LOG_ID", "tfm-log-v1")).alias("log_id"),
        "kafka_topic", "kafka_partition", "kafka_offset")), 256))


def insert_missing(spark, frame, table, key):
    frame.createOrReplaceTempView("incoming_rows")
    spark.sql(f"MERGE INTO {table} t USING incoming_rows s ON t.{key} = s.{key} "
              "WHEN NOT MATCHED THEN INSERT *")


def refresh_gold(spark, valid):
    # Recompute affected windows from committed Silver, including old windows.
    keys = valid.select("source", "entity_id", F.window("event_time", "5 minutes").start.alias("window_start")).distinct()
    silver = spark.table(SILVER).withColumn("window_start", F.window("event_time", "5 minutes").start)
    gold = (silver.join(keys, ["source", "entity_id", "window_start"], "left_semi")
        .groupBy("source", "entity_id", "domain", "unit", "window_start")
        .agg(F.count("*").alias("event_count"), F.count("*").alias("valid_event_count"),
             F.count("numeric_state").alias("numeric_event_count"),
             F.avg("numeric_state").alias("avg_numeric_state"),
             F.min("numeric_state").alias("min_numeric_state"),
             F.max("numeric_state").alias("max_numeric_state"),
             F.max("event_time").alias("last_event_time"))
        .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 5 MINUTES"))
        .withColumn("event_date", F.to_date("window_start")))
    gold.createOrReplaceTempView("incoming_gold")
    spark.sql(f"""MERGE INTO {GOLD} t USING incoming_gold s
        ON t.source = s.source AND t.entity_id = s.entity_id AND t.domain = s.domain
        AND t.unit <=> s.unit AND t.window_start = s.window_start
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def process_batch(spark, raw_frame, batch_id, config):
    if raw_frame.isEmpty():
        return
    # foreachBatch can hand us a DataFrame backed by a cloned Spark session.
    # Temporary views must be registered and consumed in that same session.
    spark = raw_frame.sparkSession
    normalized = normalize_frame(raw_frame, config).dropDuplicates(["observation_id"]).cache()
    try:
        # Each table commit is atomic. A failed batch is replayed; all writes are idempotent.
        insert_missing(spark, normalized, BRONZE, "observation_id")
        # Use the committed Bronze classification on retries, even if wall clock/config changed.
        committed = spark.table(BRONZE).join(normalized.select("observation_id"), "observation_id", "left_semi").cache()
        try:
            invalid = committed.filter(F.col("quality_status") == "quarantined")
            if not invalid.isEmpty():
                insert_missing(spark, invalid, QUARANTINE, "observation_id")
            valid = committed.filter(F.col("quality_status") != "quarantined")
            if not valid.isEmpty():
                ordering = Window.partitionBy("event_id").orderBy("kafka_timestamp", "kafka_partition", "kafka_offset")
                unique = valid.withColumn("rank", F.row_number().over(ordering)).filter("rank = 1").drop("rank")
                insert_missing(spark, unique, SILVER, "event_id")
                refresh_gold(spark, valid)
            print(json.dumps({"batch_id": batch_id, "observations": committed.count(),
                              "quarantined": invalid.count(), "status": "committed"}), flush=True)
        finally:
            committed.unpersist()
    finally:
        normalized.unpersist()


def load_inventory(spark, config):
    rows = []
    inventory_path = Path(os.getenv("INVENTORY_PATH", "/data/matter_inventory.csv"))
    if inventory_path.exists():
        with inventory_path.open() as handle:
            for row in csv.DictReader(handle):
                if row["domain"] != "sensor":
                    continue
                rows.append(("matter", row["entity_id"], "sensor", row.get("area_id") or None, config.inactivity_seconds))
                if row.get("entity_status") == "active":
                    prefix = os.getenv("SYNTHETIC_ENTITY_PREFIX", "synthetic_")
                    rows.append(("synthetic", "sensor." + prefix + row["entity_id"].split(".", 1)[1],
                                 "sensor", row.get("area_id") or None, config.inactivity_seconds))
    for entity in os.getenv("HOMEKIT_SENSOR_ENTITIES", "").split(","):
        if entity.strip():
            rows.append(("homekit", entity.strip(), "sensor", None, config.inactivity_seconds))
    if rows:
        frame = spark.createDataFrame(rows, "source string, entity_id string, domain string, area_id string, inactivity_seconds long")
        frame.dropDuplicates(["source", "entity_id"]).createOrReplaceTempView("incoming_inventory")
        spark.sql(f"""MERGE INTO {INVENTORY} t USING incoming_inventory s
            ON t.source = s.source AND t.entity_id = s.entity_id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
