from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

schema = StructType() \
    .add("device_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("energy_kwh", DoubleType()) \
    .add("region", StringType())

df = spark.readStream.format("eventhubs").load()

parsed_df = df.selectExpr("CAST(body AS STRING)") \
    .select(from_json(col("body"), schema).alias("data")) \
    .select("data.*")

parsed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/chk/energy") \
    .outputMode("append") \
    .start("/mnt/bronze/energy")