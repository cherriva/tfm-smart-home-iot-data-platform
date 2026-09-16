"""Kafka -> Iceberg Bronze -> governed Silver/quarantine -> incremental Gold."""

import argparse
import os

from pyspark.sql import functions as F

from event_contract import QualityConfig
from lakehouse import initialize, load_inventory, process_batch, session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-only", action="store_true")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    spark = session()
    config = QualityConfig.from_env()
    initialize(spark)
    load_inventory(spark, config)
    if args.init_only:
        spark.stop()
        return
    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "tfm.matter.events"))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", os.getenv("LAKEHOUSE_MAX_OFFSETS_PER_TRIGGER", "5000"))
        .load().select(F.col("key").cast("string").alias("entity_key"),
            F.col("value").cast("string").alias("raw_value"), F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"))
        .withColumn("bronze_timestamp", F.current_timestamp()))
    writer = (raw.writeStream.foreachBatch(lambda frame, batch: process_batch(spark, frame, batch, config))
        .option("checkpointLocation", os.getenv("LAKEHOUSE_CHECKPOINT_PATH", "s3a://tfm-checkpoints/iceberg_v1")))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(
        processingTime=os.getenv("LAKEHOUSE_TRIGGER_INTERVAL", "30 seconds"))
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
