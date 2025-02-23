from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

topics = ['user_events']

def generate_event():
    return {
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(["click", "purchase", "view"]),
        "timestamp": time.time()
    }

while True:
    event = generate_event()
    producer.send(topics[0], value=event)
    print(f"Sent: {event}")
    time.sleep(2)




