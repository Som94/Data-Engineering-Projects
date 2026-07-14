from pyspark.sql.functions import *

energy_df = spark.read.format("delta").load("/mnt/bronze/energy")
weather_df = spark.read.format("delta").load("/mnt/bronze/weather")

clean_df = energy_df \
    .dropDuplicates(["device_id", "timestamp"]) \
    .filter(col("energy_kwh").isNotNull())

final_df = clean_df.join(weather_df, "timestamp", "left")

final_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("region") \
    .save("/mnt/silver/energy")