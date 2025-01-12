import os
from google.cloud import bigquery

current_dir = os.getcwd()
gcp_credential_path = os.getenv('gcp_credential_path', '')
root_dir = os.path.splitdrive(current_dir)[0] + os.sep 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path)


client = bigquery.Client()

table_id = f"{client.project}.gcp_bq_dataset_3.summery"

job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    source_format = bigquery.SourceFormat.CSV,
    autodetect=True
)

path = "C:\\Users\\soman\\Downloads\\2010-summary.csv"

# load_job = client.load_table_from_file(
#     path, table_id, job_config=job_config
# )
# load_job.result()

with open(path, "rb") as file:
    load_job = client.load_table_from_file(
        file, table_id, job_config=job_config
    )
    load_job.result()

destination_table = client.get_table(table_id)
print(f"Loaded rows {destination_table.num_rows}")