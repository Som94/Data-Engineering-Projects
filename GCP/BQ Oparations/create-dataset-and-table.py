import os
from google.cloud import bigquery
current_dir = os.getcwd()
gcp_credential_path = os.getenv('gcp_credential_path', '')
root_dir = os.path.splitdrive(current_dir)[0] + os.sep
# print(gcp_credential_path)
# print(root_dir)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path)

client = bigquery.Client()

dataset_id = f"{client.project}.gcp_bq_dataset_3"

dataset = bigquery.Dataset(dataset_id)
dataset.location = 'asia-south2'

dataset = client.create_dataset(dataset, timeout=30)
print(f"Created dataset {client.project}.{dataset.dataset_id}")

full_table_id = f"{client.project}.{dataset.dataset_id}.bq-demo-table-3"
 
schema = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
]
table = bigquery.Table(full_table_id, schema=schema)
try:
    table = client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
except Exception as e:
    print(f"Failed to create table: {e}")