import json
import logging
import os

from confluent_kafka import Consumer
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("server_name", "")
TOPIC_NAME = os.getenv("topic_name", "")
BUCKET_NAME = os.getenv("bucket_name", "")
GCP_CREDENTIAL_PATH = os.getenv("gcp_credential_path", "")

print("GCP_CREDENTIAL_PATH : ", GCP_CREDENTIAL_PATH)

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": "stock_market_group",
    "auto.offset.reset": "earliest",
}


def set_gcp_credentials():
    logging.info("Setting GCP credentials...")
    if GCP_CREDENTIAL_PATH:
        current_dir = os.getcwd()
        print("current_dir ===> ", current_dir)
        gcp_credential_path = os.getenv("gcp_credential_path", "")
        print("gcp_credential_path ===> ", gcp_credential_path)
        root_dir = os.path.splitdrive(current_dir)[0] + os.sep
        print("root_dir ===> ", root_dir)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
            root_dir, gcp_credential_path
        )
        logging.info("Setting GCP credentials successfully.")
    else:
        logging.error("GCP credential path is not set.")


def main():
    set_gcp_credentials()

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])
    print("Listening for messages...")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    message_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            print(f"Received Message: {data}")

            file_name = f"stock_market_{message_count}.json"
            blob = bucket.blob(file_name)
            blob.upload_from_string(json.dumps(data), content_type="application/json")

            print(f"Saved message to GCS: gs://{BUCKET_NAME}/{file_name}")
            message_count += 1

    except Exception as e:
        print(f"Error occurred: {e}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
