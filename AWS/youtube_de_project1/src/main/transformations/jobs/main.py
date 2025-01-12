from ctypes import Structure
import datetime
import shutil
import sys
from pathlib import Path
from pyspark.sql.types import StructField, IntegerType, StructType, StringType, DateType, FloatType
from pyspark.sql.functions import concat_ws, lit, expr

# Add the project root to sys.path
project_root = Path(__file__).resolve().parents[4]
print(project_root)
sys.path.append(str(project_root))

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.write.parquet_writer import ParquetWriter
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.read.database_read import DatabaseReader
from src.main.move.move_files import move_s3_to_s3
from src.main.utility.spark_session import spark_session
from src.main.download.aws_file_download import S3FileDownloader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import *
import os
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.read.aws_read import *

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()
bucket_name = config.bucket_name

response = s3_client.list_buckets()
print(response)
logger.info("List of buckets: %s",response['Buckets'])

local_directory = config.local_directory
csv_files = [file for file in os.listdir(local_directory) if file.endswith('.csv')]
connection = get_mysql_connection()
cursor = connection.cursor()
 
total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    
    statement = f'''
        select distinct file_name 
        from {config.database_name}.{config.product_staging_table}
        where file_name in ({str(total_csv_files)[1:-1]}) and status="A"
    '''
    logger.info(f"Dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No records matched.")
else:
    logger.info("Last run was successful!")

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 bucket_name,
                                                 folder_path = folder_path
                                                 )
    logger.info("Absolute path on s3 bucket for csv file %s",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")
except Exception as e:
    logger.error("Exited with the error:- %s", e)
    raise e


prefix = f"s3://{bucket_name}"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
# logging.info(f"File path available on s3 under %s bucket and folder name is %s")
logging.info(f"File path available on s3 under {bucket_name} and folder name is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

all_files = os.listdir(local_directory)
logger.info(f"List of files present at my locsl directory after download : {all_files}")

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request.")
        raise Exception("No csv data available to process the request.")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("****************** Listing the Files ****************************")
logger.info("List of csv files that need to be processed %s", csv_files)

logger.info("****************** Creating Spark Session ****************************")
spark = spark_session()
logger.info("****************** Spark Session Created ****************************")


# CHeck the required columns in the schema of csv files
# If not keep it in a list or error_files
# else union all the data into one dataframe

logger.info("**************** Checking for schema for data loaded in s3 *****************")

correct_files = []

for data in csv_files:
    data_schema = spark.read.format("csv")\
                    .option("header", "true")\
                    .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns as {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)

logger.info(f"*************** List of correct files ****************")
logger.info(f"*************** List of error files ****************")
logger.info(f"*************** Moving error data to error directory if any ****************")


# Move data to error directory on local

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(config.error_folder_path_local, file_name)
            print("file_name line no 149 :", file_name)
            print("file_path line no 150 :", file_path)
            print("destination_path line no 151 :", destination_path)
            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' from s3 file path to '{destination_path}' ")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix,destination_prefix, file_name)
            logger.info(f"message = {message}")
else:
    logger.info(f"************ There is no error files.. ************")


# Additional columns needs to be taken care of
# Determine extra columns

#Before running the process
# stage table need to be updated with the status as Active(A) or Inactive(I)

logger.info(f"************** Updating the product staging table that we have started the process ************")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
print("correct_files Line no 175 ===> ", correct_files)
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        # file_path = file.replace("\\", "/")
        # statements = f"INSERT INTO {db_name}.{config.product_staging_table} "\
        #             f"(file_name, file_location, created_date, status)"\
        #             f"VALUES ('{file_name}', '{file}', '{formatted_date}', 'A')"
        statements = (
            f"INSERT INTO {db_name}.{config.product_staging_table} "
            f"(file_name, file_location, created_date, status) "
            f"VALUES ('{file_name}', '{file}', '{formatted_date}', 'A')"
        )
        insert_statements.append(statements)
    
    logger.info(f"Insert statement created for staging table ===> {insert_statements}")
    logger.info(f"******** Connecting with MySQL server *********")

    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("************* My SQL server connected successfully ************** ")

    for statement in insert_statements:
        print("Line no 199 statement ===> ", statement)
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("************** There is no files to process ***************")
    raise Exception("*********** No data available with correct files *************")


logger.info("************** Staging table updated successfully ***************")
logger.info("************** Fixing extra columns coming from source ***************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

#COnnecting with DatabaseReader

# database_client = DatabaseReader(config.url, config.properties)
logger.info("************** creating empty dataframe **************")
# final_df_to_process = database_client.create_dataframe(spark, "empty_df_create")

final_df_to_process = spark.createDataFrame([], schema = schema)
# Create a new column with concatenated values of extra columns

for data in correct_files:
    data_df = spark.read.format('csv')\
        .option('header','true')\
        .option('inferSchema', 'true')\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present as source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        logger.info(f"preocessed {data} and added 'additional_column'")
    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        
    final_df_to_process = final_df_to_process.union(data_df)
logger.info("************** Final DataFrame from source which will going to processing **************")
final_df_to_process.show()
# spark.stop()

# Enrich the data from all dimension table
# also create a datamart for sales_team and their incentive, address and all
# another data mart for customer who bought how much each days of month
# for every month there should be a file and inside that
# there should be a store_id segrigation
#Read the data from perquet and generate a csv file
# in which there will be a sales_person_name, sales_person_store_id,
# sales_person_total_billing_done_for_each_month, total_incentive


#connecting with DatabaseReader 
database_client = DatabaseReader(config.url, config.properties)
# creating df for all tables

#Customer Table
logger.info("************* Loading customer table into customer_table_df ****************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

#Product Table
logger.info("************* Loading customer table into product_table_df ****************")
product_table_df = database_client.create_dataframe(spark, config.product_table)

#Product Staging Table
logger.info("************* Loading customer table into product_staging_table_df ****************")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

#Sale team Table
logger.info("************* Loading customer table into sales_team_table_df ****************")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

#Store Table
logger.info("************* Loading customer table into store_table_df ****************")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_joins = dimesions_table_join(
                                    final_df_to_process,
                                    customer_table_df,
                                    store_table_df,
                                    sales_team_table_df
                                )

logger.info("************** Final Enriched Data ***************")
s3_customer_store_sales_df_joins.show()

#Write the customer data into customer data mart in parquet format
# file will be written to local first
# Move the RAW data to s3 bucket for reporting tool
# Write the reporting data into MySQL table also

logger.info("************** Write the data into customer data mart *****************")
final_customer_data_mart_df = s3_customer_store_sales_df_joins\
                    .select("ct.customer_id",
                            "ct.first_name",
                            "ct.last_name",
                            "ct.address",
                            "ct.pincode",
                            "sales_date",
                            "total_cost"
                            )
logger.info("************** Final data for customer data mart *****************")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"************** Customer data written to local disk at {config.customer_data_mart_local_file} *****************")

# MOve data on s3 bucket for customer_data_mart
logger.info(f"************** Data movement from local to s3 for customer data mart *****************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")


#Sales_team Data mart
logger.info("************** Write the data into sales_team data mart *****************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_joins\
                    .select(
                        "store_id",
                        "sales_persion_id",
                        "sales_persion_first_name",
                        "sales_persion_last_name",
                        "store_manager_name",
                        "manager_id",
                        "is_manager",
                        "sales_persion_address",
                        "sales_persion_pincode",
                        "sales_date",
                        "total_cost",
                        expr("SUBSTRING(sales_date,1,7) as sales_month")
                    )
logger.info("************** Final data for sales team data mart *****************")
final_sales_team_data_mart_df.show()

parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)

logger.info(f"************** Sales team data written to local disk at {config.sales_team_data_mart_local_file} *****************")

# MOve data on s3 bucket for sales tema data mart
logger.info(f"************** Data movement from local to s3 for sales team data mart *****************")
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#Also writting the data in to partitioon
final_sales_team_data_mart_df.write.format("parquet")\
    .option("header", "true")\
    .mode("overwrite")\
    .partitionBy("sales_month", "store_id")\
    .option("path", config.sales_team_data_mart_partitioned_local_file)\
    .save()

# Move data on s3 for partitioned folder
s3_prefixx = "sales_patitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print("FILE ===> ", file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_local_file)
        s3_key = f"{s3_prefixx}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

#Calculation for customer mart
# find out the customer total purchase every month
# Write the data into MySQL table

logger.info("*********** Calculating customer every month purchased amount ***********")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("*********** Calculation of customer data mart done and written into the table ***********")

#Calculation for sales team mart
# find out the total sales done by each sales person every month
# Give the top perfomer 1% inentive of total sales of the month
#Rest sales man will get nothing
#write the data into mysql table

logger.info("*********** Calculating sales every month billed amount ***********")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("*********** Calculation of sales mart done and written into the table ***********")



################## Last Step ##################
#Move the filr on s3 into processed folder and delete the local files

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory

message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info(f"*********** Delete sales data from local *************")
delete_local_file(config.local_directory)
logger.info(f"*********** Deleted sales data from local *************")

logger.info(f"*********** Delete customer_data_mart_local_file *************")
delete_local_file(config.customer_data_mart_local_file)
logger.info(f"*********** Deleted customer_data_mart_local_file *************")


logger.info(f"*********** Delete sales_team_data_mart_local_file *************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info(f"*********** Deleted sales_team_data_mart_local_file *************")

logger.info(f"*********** Delete sales_team_data_mart_partitioned_local_file *************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info(f"*********** Deleted sales_team_data_mart_partitioned_local_file *************")


update_statements = []
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statements = f'''
                UPDATE {db_name}.{config.product_staging_table}
                SET status = "I", updated_date = "{formatted_date}"
                WHERE file_name = "{file_name}"
            '''
        update_statements.append(statements)
    logger.info(f"Update statement created for staging table --- {update_statements}")
    logger.info(f"************** Connecting with MySQL server *************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info(f"************** Successfully Connected with MySQL server *************")

    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info(f"************** There is some error in process in between *************")
    sys.exit()

input("Press enter to terminate")

