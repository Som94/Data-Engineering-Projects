import os

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("server_name", "")
TOPIC_NAME = os.getenv("topic_name", "")


print("BOOTSTRAP_SERVER : ", BOOTSTRAP_SERVER)
print("TOPIC_NAME : ", TOPIC_NAME)

admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
fs = admin_client.create_topics([topic])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created successfully!")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
