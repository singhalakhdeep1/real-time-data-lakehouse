from confluent_kafka import Producer
import json
import random
import time
import uuid
from datetime import datetime

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)


def generate_order_event():
    """Generate a realistic e-commerce order event"""
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1, 1000)}",
        "product_id": f"prod_{random.randint(1, 500)}",
        "amount": round(random.uniform(10, 500), 2),
        "status": random.choice(["created", "paid", "shipped", "delivered"]),
        "timestamp": datetime.utcnow().isoformat()
    }


def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == "__main__":
    print("Starting e-commerce event producer...")
    print("Publishing events to Kafka topic 'orders'")
    
    event_count = 0
    try:
        while True:
            event = generate_order_event()
            
            # Produce message to Kafka
            producer.produce(
                'orders',
                key=event['order_id'],
                value=json.dumps(event),
                callback=delivery_report
            )
            
            # Poll to handle delivery reports
            producer.poll(0)
            
            event_count += 1
            if event_count % 10 == 0:
                print(f"Published {event_count} events...")
            
            # Sleep to control event rate (10 events/second)
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Flush any remaining messages
        producer.flush()
        print(f"Total events published: {event_count}")
