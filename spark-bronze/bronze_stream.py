"""Spark Structured Streaming consumer for the TFM Bronze layer."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tfm.matter.events")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.environ["MINIO_ROOT_USER"]
MINIO_ROOT_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://tfm-bronze/matter_events")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://tfm-checkpoints/matter_events")


event_schema = StructType(
    [
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
    ]
)


spark = (
    SparkSession.builder.appName("TFM-Matter-Bronze")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


raw_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze_events = (
    raw_events.select(
        F.col("key").cast("string").alias("entity_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("event", F.from_json(F.col("raw_value"), event_schema))
    .select(
        "entity_key",
        "raw_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "event.*",
    )
    .withColumn("bronze_timestamp", F.current_timestamp())
    .withColumn(
        "event_date",
        F.to_date(
            F.coalesce(
                F.to_timestamp("event_timestamp"),
                F.col("kafka_timestamp"),
                F.current_timestamp(),
            )
        ),
    )
)


query = (
    bronze_events.writeStream.format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("event_date", "domain")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
