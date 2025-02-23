import os

from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("server_name", "")
TOPIC_NAME = os.getenv("topic_name", "")
print("BOOTSTRAP_SERVER : ", BOOTSTRAP_SERVER)
print("TOPIC_NAME : ", TOPIC_NAME)
admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
admin_client.delete_topics([TOPIC_NAME])

print("Topic 'stock_market_topic' deleted successfully!")
