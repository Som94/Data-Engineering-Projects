import json

import yaml
from google.cloud import pubsub_v1

# Load Config
with open("configs/config.yaml") as f:
    config = yaml.safe_load(f)

topic_name = config["gcp"]["pubsub_topic"]
publisher = pubsub_v1.PublisherClient()


def publish_message(data):
    message_json = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_name, message_json)
    return future.result()
