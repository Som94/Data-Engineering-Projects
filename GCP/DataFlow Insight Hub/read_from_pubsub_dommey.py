from google.cloud import pubsub_v1
import os
import json 
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
# from apache_beam.io.gcp.bigquery import WriteToBigQuery
from dotenv import load_dotenv
from create_dataset_and_table_in_bq import create_bq_dataset_and_table
load_dotenv()

current_dir = os.getcwd()
gcp_credential_path = os.getenv('gcp_credential_path', '')
project_id = os.getenv('project_id', '') 
subscription_name = os.getenv('subscription_name', '')  

# Get the root of the current drive
root_dir = os.path.splitdrive(current_dir)[0] + os.sep
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path)
 
# Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Define the subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()

# Subscribe to the subscription and start receiving messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the subscriber running indefinitely
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
