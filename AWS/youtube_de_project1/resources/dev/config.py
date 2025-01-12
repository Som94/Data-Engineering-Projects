import os

key = "my_aws_de_project1"
iv = "aws_de_project_1"
salt = "my_aws_de_project1_AesEncryption"

#AWS Access And Secret key
aws_access_key = "SxQAIkshFjEIOI9dYDPI5SJzGUGG+6aQW82pSdyTczE="
aws_secret_key = "fA6ezIqdKN2xbxUz0P4KZ3CZBew5PgyQ+qT6fF0cv6TKpT1GiDr+wo5F++acDymC"
bucket_name = "som-de-project-bucket1"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
host = "localhost"
user = "root"
password = "root" 
database_name = "youtube_project"
url = f"jdbc:mysql://{host}:3306/{database_name}"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}
java_mysql_connector = "C:\\mysql-connector-java\\mysql-connector-j-9.1.0.jar"
# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "D:\\git_repo\\Data-Engineering-Projects\\AWS\\Project1\\all_files\\file_from_s3\\" 
customer_data_mart_local_file = "D:\\git_repo\\Data-Engineering-Projects\\AWS\\Project1\\all_files\\customer_data_mart\\"
sales_team_data_mart_local_file = "D:\\git_repo\\Data-Engineering-Projects\\AWS\\Project1\\all_files\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "D:\\git_repo\\Data-Engineering-Projects\\AWS\\Project1\\all_files\\sales_partition_data\\"
error_folder_path_local = "D:\\git_repo\\Data-Engineering-Projects\\AWS\\Project1\\all_files\\error_files\\"
