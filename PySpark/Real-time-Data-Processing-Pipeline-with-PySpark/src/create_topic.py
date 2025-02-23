from confluent_kafka.admin import AdminClient, NewTopic

# Kafka broker (Make sure Kafka is running)
BOOTSTRAP_SERVER = "localhost:9092"

# Define topic name
TOPIC_NAME = "user_events"

# Create an admin client
admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})

# Create a new topic
topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)

# Send create request
fs = admin_client.create_topics([topic])
print("fs ======> ")
print(fs)
# Check the result
for topic, f in fs.items():
    try:
        f.result()  # If successful, this will not raise an exception
        print(f"Topic '{topic}' created successfully!")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
