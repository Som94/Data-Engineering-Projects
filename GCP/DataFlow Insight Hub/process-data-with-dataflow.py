import os
import json 
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from dotenv import load_dotenv
from create_dataset_and_table_in_bq import create_bq_dataset_and_table
load_dotenv()
current_dir = os.getcwd()
gcp_credential_path = os.getenv('gcp_credential_path', '')
project_id = os.getenv('project_id', '')
topic_name = os.getenv('topic_name', '')
region = os.getenv('region', '')
bucket_path = os.getenv('bucket_path', '')
job_name = os.getenv('job_name', '')
dataset_id = os.getenv('dataset_id', '')
table_id = os.getenv('table_id', '')
root_dir = os.path.splitdrive(current_dir)[0] + os.sep
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path)
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = region
google_cloud_options.job_name = job_name 
google_cloud_options.staging_location = f"{bucket_path}/staging"
google_cloud_options.temp_location = f"{bucket_path}/temp"
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(StandardOptions).streaming = True
def transform_data(element): 
    return json.loads(element.data.decode('utf-8'))
create_bq_dataset_and_table(region, dataset_id, table_id)
schema = ('primary_title:STRING, num_votes:INTEGER') 
subscription_name = f'projects/{project_id}/subscriptions/dataflow-insight-hub-pubsub-topic-2-sub'
with beam.Pipeline(options=options) as p:
    (p
    | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=f'projects/{project_id}/topics/{topic_name}', with_attributes=True)
    | 'Transform Data' >> beam.Map(transform_data)
    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=f"{project_id}:{dataset_id}.{table_id}",
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )