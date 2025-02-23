import os

from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("server_name", "")

admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})

topics = admin_client.list_topics().topics
print("Available Topics:", topics)

for topic in topics:
    print(f"- {topic}")
