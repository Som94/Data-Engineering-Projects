from google.cloud import pubsub_v1 
import pandas as pd
import os
from io import StringIO
from google.cloud import pubsub_v1
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()
current_dir = os.getcwd()
gcp_credential_path = os.getenv('gcp_credential_path', '')
project_id = os.getenv('project_id', '')
topic_name = os.getenv('topic_name', '')
bucket_name = os.getenv('bucket_name', '') 
csv_file_path =  os.getenv('csv_file_path', '')
root_dir = os.path.splitdrive(current_dir)[0] + os.sep 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path) 
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name) 
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(csv_file_path)
csv_data = blob.download_as_text()
dataframe = pd.read_csv(StringIO(csv_data))
for index, row in dataframe.iterrows():
    message = row.to_json() 
    future = publisher.publish(topic_path, message.encode('utf-8'))
