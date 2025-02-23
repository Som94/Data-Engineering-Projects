from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4") \
    .getOrCreate()

# Define Schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

print("DF:")

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

print("df.show() :")
df.printSchema()  # Use printSchema() instead of df.show() for streaming data

# Write streaming data to local drive in Parquet format
query = df.writeStream \
    .format("parquet") \
    .option("path", "D:/git_repo/PySpark/Kafka-Streaming-Data/local_output/user_events/processed/") \
    .option("checkpointLocation", "D:/git_repo/PySpark/Kafka-Streaming-Data/local_output/user_events/checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
