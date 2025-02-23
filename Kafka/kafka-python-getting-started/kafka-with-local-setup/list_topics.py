# from confluent_kafka.admin import AdminClient

# admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
# topics = admin_client.list_topics().topics

# print("Available Kafka Topics: ", topics)
# for topic in topics:
#     print(f"- {topic}")


from confluent_kafka.admin import AdminClient

# Kafka broker
BOOTSTRAP_SERVER = "localhost:9092"

# Create admin client
admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})

# List topics
topics = admin_client.list_topics().topics
print("Available Topics:", topics)

for topic in topics:
    print(f"- {topic}")