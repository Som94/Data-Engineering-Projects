import json
import logging
import os
import time
from typing import Any

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("server_name", "")
TOPIC_NAME = os.getenv("topic_name", "")
FILE_PATH = os.getenv("csv_file_path", "")


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

producer_config = {"bootstrap.servers": BOOTSTRAP_SERVER}
producer = Producer(producer_config)


def load_data(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    logger.info(f"Data loaded successfully. Sample data:\n{df.head()}")
    return df


def delivery_callback(err: Any, msg: Any) -> None:
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_messages(producer: Producer, topic: str, data: pd.DataFrame) -> None:
    try:
        while True:
            record = data.sample(1).to_dict(orient="records")[0]
            json_record = json.dumps(record).encode("utf-8")

            producer.produce(topic, value=json_record, callback=delivery_callback)
            producer.poll(0)

            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Message production interrupted by user.")
    finally:
        producer.flush()
        logger.info("All messages sent successfully.")


def main() -> None:
    logger.info(f"Loading data from: {FILE_PATH}")
    data = load_data(FILE_PATH)

    logger.info(f"Producing messages to topic: {TOPIC_NAME}")
    produce_messages(producer, TOPIC_NAME, data)


if __name__ == "__main__":
    main()
