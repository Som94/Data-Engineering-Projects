from confluent_kafka.admin import AdminClient

admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})

# Delete the topic
admin_client.delete_topics(["my-python-topic"])

print("Topic 'my-python-topic' deleted successfully!")
