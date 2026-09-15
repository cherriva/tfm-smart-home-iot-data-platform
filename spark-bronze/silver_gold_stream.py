"""Silver normalization and Gold 5-minute metrics for the TFM."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://tfm-bronze/matter_events")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://tfm-silver/matter_events")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://tfm-gold/entity_5m")
SILVER_CHECKPOINT_PATH = os.getenv(
    "SILVER_CHECKPOINT_PATH", "s3a://tfm-checkpoints/silver_matter_events"
)
GOLD_CHECKPOINT_PATH = os.getenv(
    "GOLD_CHECKPOINT_PATH", "s3a://tfm-checkpoints/gold_entity_5m"
)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.environ["MINIO_ROOT_USER"]
MINIO_ROOT_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
MAX_FILES_PER_TRIGGER = int(os.getenv("SILVER_GOLD_MAX_FILES_PER_TRIGGER", "500"))


bronze_schema = StructType(
    [
        StructField("entity_key", StringType(), True),
        StructField("raw_value", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("state", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("updated_timestamp", StringType(), True),
        StructField("mqtt_topic", StringType(), True),
        StructField("bridge_timestamp", StringType(), True),
        StructField("attributes", MapType(StringType(), StringType(), True), True),
        StructField("bronze_timestamp", TimestampType(), True),
        StructField("event_date", DateType(), True),
    ]
)


spark = (
    SparkSession.builder.appName("TFM-Matter-Silver-Gold")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


bronze_events = (
    spark.readStream.schema(bronze_schema)
    .format("parquet")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .load(BRONZE_PATH)
)

event_time = F.coalesce(F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"))
numeric_pattern = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$"

normalized_events = (
    bronze_events.withColumn("event_time", event_time)
    .withColumn("ingestion_time", F.to_timestamp("ingestion_timestamp"))
    .withColumn("updated_time", F.to_timestamp("updated_timestamp"))
    .withColumn("unit", F.col("attributes").getItem("unit_of_measurement"))
    .withColumn("device_class", F.col("attributes").getItem("device_class"))
    .withColumn("friendly_name", F.col("attributes").getItem("friendly_name"))
    .withColumn(
        "numeric_state",
        F.when(F.col("state").rlike(numeric_pattern), F.col("state").cast("double")),
    )
    .withColumn(
        "quality_flag",
        F.when(F.col("entity_id").isNull(), F.lit("missing_entity"))
        .when(F.col("state").isNull(), F.lit("missing_state"))
        .when(F.col("state").isin("unknown", "unavailable"), F.lit("unavailable"))
        .when(F.col("event_time").isNull(), F.lit("missing_event_time"))
        .otherwise(F.lit("ok")),
    )
    .withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("entity_id"), F.lit("")),
                F.coalesce(F.col("event_timestamp"), F.lit("")),
                F.coalesce(F.col("state"), F.lit("")),
                F.coalesce(F.col("kafka_topic"), F.lit("")),
                F.coalesce(F.col("kafka_partition").cast("string"), F.lit("")),
                F.coalesce(F.col("kafka_offset").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn("event_date", F.to_date("event_time"))
    .select(
        "event_id",
        "entity_id",
        "entity_key",
        "domain",
        "state",
        "numeric_state",
        "unit",
        "device_class",
        "friendly_name",
        "attributes",
        "quality_flag",
        "event_time",
        "ingestion_time",
        "updated_time",
        "bronze_timestamp",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "mqtt_topic",
        "bridge_timestamp",
        "raw_value",
        "event_date",
    )
)


silver_query = (
    normalized_events.writeStream.format("parquet")
    .outputMode("append")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .partitionBy("event_date", "domain")
    .trigger(processingTime="10 seconds")
    .start()
)


gold_events = (
    normalized_events.filter(F.col("event_time").isNotNull())
    .groupBy(
        F.window("event_time", "5 minutes"),
        F.col("entity_id"),
        F.col("domain"),
        F.col("unit"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("quality_flag") == "ok", 1).otherwise(0)).alias(
            "valid_event_count"
        ),
        F.count("numeric_state").alias("numeric_event_count"),
        F.avg("numeric_state").alias("avg_numeric_state"),
        F.min("numeric_state").alias("min_numeric_state"),
        F.max("numeric_state").alias("max_numeric_state"),
        F.max("event_time").alias("last_event_time"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "entity_id",
        "domain",
        "unit",
        "event_count",
        "valid_event_count",
        "numeric_event_count",
        "avg_numeric_state",
        "min_numeric_state",
        "max_numeric_state",
        "last_event_time",
    )
    .withColumn("event_date", F.to_date("window_start"))
)


def write_gold_snapshot(batch_df, batch_id):
    """Replace Gold with the complete 5-minute aggregate known by this query."""
    (
        batch_df.write.mode("overwrite")
        .partitionBy("event_date", "domain")
        .parquet(GOLD_PATH)
    )


gold_query = (
    gold_events.writeStream.foreachBatch(write_gold_snapshot)
    .outputMode("complete")
    .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)


spark.streams.awaitAnyTermination()
