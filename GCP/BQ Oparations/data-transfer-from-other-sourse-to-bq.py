import os
from google.cloud import bigquery_datatransfer
from google.protobuf.timestamp_pb2 import Timestamp
import logging
import datetime

def set_gcp_credentials():
    logging.info("set_gcp_credentials() called.")
    current_dir = os.getcwd()
    gcp_credential_path = os.getenv('gcp_credential_path', '')
    root_dir = os.path.splitdrive(current_dir)[0] + os.sep
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, gcp_credential_path)
    logging.info("set_gcp_credentials() end.")


def get_transfer_client():
    """
    Initialize and return the BigQuery Data Transfer Service client.
    """
    logging.info("get_transfer_client() called.")
    return bigquery_datatransfer.DataTransferServiceClient()


def create_transfer_config(client, project_id, dataset_id, table_name, s3_uri, aws_access_key, aws_secret_key):
    """
    Create a transfer configuration for transferring data from AWS S3 to BigQuery.
    
    Args:
        client: Data Transfer Service client.
        project_id: Google Cloud Project ID.
        dataset_id: BigQuery Dataset ID.
        s3_uri: AWS S3 URI.
        aws_access_key: AWS Access Key ID.
        aws_secret_key: AWS Secret Access Key.

    Returns:
        The created transfer configuration.
    """
    logging.info("create_transfer_config() called.")
    run_date = datetime.datetime.now().strftime("%Y%m%d")
    transfer_config = bigquery_datatransfer.TransferConfig(
        display_name="S3 to BigQuery Transfer",
        data_source_id="amazon_s3",
        destination_dataset_id=dataset_id,
        params={ 
            "access_key_id": aws_access_key,
            "secret_access_key": aws_secret_key,
            "destination_table_name_template": table_name,
            "data_path": s3_uri,
            "file_format": "CSV",
            "ignore_unknown_values":"true",
            "field_delimiter":",",
            "skip_leading_rows":"1",
            "allow_quoted_newlines":"true",
            "allow_jagged_rows":"true"
        },
    ) 


    return client.create_transfer_config(
        parent=f"projects/{project_id}",
        transfer_config=transfer_config,
    )
    logging.info("create_transfer_config() after return line no 53.")



def start_transfer_job(client, transfer_config_name):
    """
    Start a manual transfer run for the given transfer configuration.
    
    Args:
        client: Data Transfer Service client.
        transfer_config_name: The name of the transfer configuration.
    """
    logging.info("start_transfer_job() called.")
    requested_run_time = Timestamp()
    requested_run_time.FromDatetime(datetime.datetime.utcnow())
    request = bigquery_datatransfer.StartManualTransferRunsRequest(
        parent=transfer_config_name,
        requested_run_time=requested_run_time,
    ) 
    response = client.start_manual_transfer_runs(request=request)
    print("Manual transfer run started:")
    for run in response.runs:
        print(f"Run ID: {run.name}")
    logging.info("start_transfer_job() end.")
    


def list_transfer_runs(client, transfer_config_name):
    """
    List all transfer runs for the given transfer configuration and display their states.
    
    Args:
        client: Data Transfer Service client.
        transfer_config_name: The name of the transfer configuration.
    """
    logging.info("list_transfer_runs() called.")

    transfer_runs = client.list_transfer_runs(parent=transfer_config_name)
    for run in transfer_runs:
        state = bigquery_datatransfer.TransferState(run.state).name
        print(f"Run: {run.name}, State: {state}")
    logging.info("list_transfer_runs() end.")


def main(): 
    # Set up GCP credentials
    set_gcp_credentials()

    # Initialize the transfer client
    transfer_client = get_transfer_client()

    # Load environment variables
    project_id = os.getenv('project_id')
    dataset_id = os.getenv("bq_data_transfer_dataset_id")
    table_name = os.getenv('bq_table_name')
    s3_uri = os.getenv("s3_uri")
    aws_access_key = os.getenv("access_key")
    aws_secret_key = os.getenv("secret_access_key")

    if not (project_id and dataset_id and s3_uri and aws_access_key and aws_secret_key):
        raise ValueError("One or more required environment variables are missing!")

    # Create the transfer configuration
    transfer_config = create_transfer_config(
        client=transfer_client,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=table_name,
        s3_uri=s3_uri,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
    )
    print("Transfer configuration created:", transfer_config.name)

    # Start the transfer job
    start_transfer_job(client=transfer_client, transfer_config_name=transfer_config.name)

    # List transfer runs
    list_transfer_runs(client=transfer_client, transfer_config_name=transfer_config.name)


if __name__ == "__main__":
    main()
