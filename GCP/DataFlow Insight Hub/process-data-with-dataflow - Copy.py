import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from dotenv import load_dotenv
from create_dataset_and_table_in_bq import create_bq_dataset_and_table

# Load environment variables from a .env file
load_dotenv()

# Retrieve environment variables
current_dir = os.getcwd()
gcp_credential_path = os.getenv('gcp_credential_path', '')
project_id = os.getenv('project_id', '')
topic_name = os.getenv('topic_name', '')
region = os.getenv('region', '')
bucket_path = os.getenv('bucket_path', '')
job_name = os.getenv('job_name', '')
dataset_id = os.getenv('dataset_id', '')
table_id = os.getenv('table_id', '')

# Set the Google Application Credentials environment variable
root_dir = os.path.splitdrive(current_dir)[0] + os.sep
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path)

# Configure pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = region
google_cloud_options.job_name = job_name
google_cloud_options.staging_location = f"{bucket_path}/staging"
google_cloud_options.temp_location = f"{bucket_path}/temp"
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(StandardOptions).streaming = True

# Function to transform the data
def transform_data(element):
    print(f"Received message: {element}")
    return json.loads(element.decode('utf-8'))

# Create BigQuery dataset and table
dataset_created, table_created = create_bq_dataset_and_table(region, dataset_id, table_id)

# BigQuery schema definition
schema = 'primary_title:STRING, num_votes:INTEGER'

# Function to log and transform data
def log_and_transform_data(element):
    print(f"Received message: {element}")
    return json.loads(element.data.decode('utf-8'))

# Define and run the pipeline
if (dataset_created and table_created) or not (dataset_created and table_created):
    with beam.Pipeline(options=options) as p:
        messages = p | 'Create Test Data' >> beam.Create([
            b'{"primary_title": "Test Movie", "num_votes": 1000}',
            b'{"primary_title": "Another Movie", "num_votes": 500}'
        ])
        (messages
        | 'Log and Transform Data' >> beam.Map(log_and_transform_data)
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=f"{project_id}:{dataset_id}.{table_id}",
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
