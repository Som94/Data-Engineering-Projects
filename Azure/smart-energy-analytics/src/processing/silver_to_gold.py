from pyspark.sql.functions import *

df = spark.read.format("delta").load("/mnt/silver/energy")

gold_df = df.groupBy(
    "region",
    window("timestamp", "1 hour")
).agg(
    sum("energy_kwh").alias("total_energy"),
    avg("temperature").alias("avg_temp")
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/gold/energy")