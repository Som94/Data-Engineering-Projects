import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *
from resources.dev import config

def spark_session():
    print("spark_session() called ....")
    spark = SparkSession.builder.master("local[*]")\
        .appName("som_spark2")\
        .config("spark.driver.extraClassPath", config.java_mysql_connector)\
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()
    # spark.sparkContext.setLogLevel("DEBUG")
    logger.info("spark session %s",spark)
    print("************* Version *********************")
    print(spark.version)
    print(sys.version)
    print("*************** End *******************")
    return spark