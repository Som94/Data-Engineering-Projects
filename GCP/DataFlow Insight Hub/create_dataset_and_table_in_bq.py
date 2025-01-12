from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv() 

def create_bq_dataset_and_table(region, dataset_id, table_id):
    client = bigquery.Client()
    dataset_created = False
    table_created = False

    # Create dataset if it does not exist
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = region
        dataset = client.create_dataset(dataset)  
        print(f"Created dataset {dataset_id}.")
        dataset_created = True

    # Create table if it does not exist
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception as e:
        schema = [
            bigquery.SchemaField("primary_title", "STRING"),
            bigquery.SchemaField("num_votes", "INTEGER"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(f"Created table {table_id}.")
        table_created = True

    return dataset_created, table_created

