from confluent_kafka import Producer

# Kafka Configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Callback function to confirm message delivery
def acked(err, msg):
    if err:
        print(f"Message failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Sending messages
for i in range(5):
    producer.produce('my-python-topic', key=str(i), value=f"Hello Kafka {i}", callback=acked)
    producer.flush()  # Ensure messages are sent

print("Messages sent successfully!")
