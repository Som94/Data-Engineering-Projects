import requests
import json
from pyspark.sql import SparkSession

url = "https://api.weather.com/data"
response = requests.get(url)
data = response.json()

df = spark.createDataFrame(data)

df.write.format("delta").mode("append").save("/mnt/bronze/weather")