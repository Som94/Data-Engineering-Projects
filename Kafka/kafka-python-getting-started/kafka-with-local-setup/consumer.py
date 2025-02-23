from confluent_kafka import Consumer, KafkaException

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'  # Read messages from the beginning
}

consumer = Consumer(conf)
consumer.subscribe(['my-python-topic'])

try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(f"Received: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
