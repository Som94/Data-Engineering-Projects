from pyspark.sql.functions import col, to_timestamp

transformed_df = batch_df.withColumn("event_time", to_timestamp(col("timestamp")))
transformed_df.write.parquet("s3://your-bucket/transformed/")
