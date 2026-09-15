"""Load AEMET normalized JSONL objects from MinIO into an Iceberg table."""
import os
from pyspark.sql import functions as F
from lakehouse import session, initialize, AEMET_DAILY

spark = session("TFM-AEMET-Silver")
initialize(spark)
path = "s3a://" + os.getenv("AEMET_SILVER_BUCKET", "tfm-silver") + "/aemet/daily/"
incoming = (spark.read.json(path)
    .withColumn("observation_date", F.to_date("observation_date"))
    .withColumn("loaded_at", F.to_timestamp("loaded_at")))
incoming.createOrReplaceTempView("aemet_incoming")
spark.sql(f"""MERGE INTO {AEMET_DAILY} t USING aemet_incoming s
 ON t.observation_date=s.observation_date AND t.station_id=s.station_id
 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
print(incoming.count(), "AEMET rows loaded into", AEMET_DAILY, flush=True)
spark.stop()
