batch_df = spark.read.csv("s3://your-bucket/raw_data/local_data.csv", header=True, inferSchema=True)
batch_df.show()

 