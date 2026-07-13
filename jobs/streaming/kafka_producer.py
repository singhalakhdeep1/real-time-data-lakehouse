"""
Kafka Streaming Producer for Real-time Data Ingestion
Produces streaming data to Kafka topics for real-time processing
"""

import json
import time
import random
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer
import os


class StreamingProducer:
    """Kafka producer for real-time data streaming"""
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            compression_type='gzip'
        )
        self.topics = {
            'events': 'raw_events',
            'transactions': 'transactions_stream',
            'user_activity': 'user_activity_stream',
            'metrics': 'system_metrics'
        }
    
    def produce_event(self, event_type: str, event_data: Dict[str, Any], key: str = None):
        """Produce a single event to Kafka"""
        topic = self.topics.get(event_type, 'raw_events')
        
        message = {
            **event_data,
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type
        }
        
        future = self.producer.send(topic, value=message, key=key)
        future.add_callback(self._on_send_success, topic)
        future.add_errback(self._on_send_error, topic)
        
        return future
    
    def produce_batch(self, events: list, event_type: str):
        """Produce a batch of events to Kafka"""
        futures = []
        for event in events:
            key = event.get('id') or event.get('user_id')
            future = self.produce_event(event_type, event, key)
            futures.append(future)
        
        # Wait for all messages to be sent
        for future in futures:
            future.get(timeout=10)
        
        return len(futures)
    
    def _on_send_success(self, topic, metadata):
        """Callback for successful message send"""
        print(f"Message sent to {topic} at offset {metadata.offset}")
    
    def _on_send_error(self, topic, excp):
        """Callback for failed message send"""
        print(f"Failed to send message to {topic}: {excp}")
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.producer.close()


class EventGenerator:
    """Generates synthetic events for testing"""
    
    def __init__(self):
        self.event_types = ['page_view', 'click', 'purchase', 'signup', 'logout']
        self.user_ids = [f"user_{i}" for i in range(1, 101)]
        self.pages = ['/home', '/products', '/checkout', '/profile', '/settings']
    
    def generate_user_activity(self, count: int = 10) -> list:
        """Generate user activity events"""
        events = []
        for _ in range(count):
            event = {
                'id': f"evt_{random.randint(100000, 999999)}",
                'user_id': random.choice(self.user_ids),
                'event_type': random.choice(self.event_types),
                'page': random.choice(self.pages),
                'session_id': f"sess_{random.randint(1000, 9999)}",
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'referrer': random.choice(['google', 'direct', 'social', 'email'])
            }
            events.append(event)
        return events
    
    def generate_transaction(self, count: int = 5) -> list:
        """Generate transaction events"""
        events = []
        for _ in range(count):
            event = {
                'id': f"txn_{random.randint(100000, 999999)}",
                'user_id': random.choice(self.user_ids),
                'amount': round(random.uniform(10, 1000), 2),
                'currency': random.choice(['USD', 'EUR', 'GBP']),
                'merchant': random.choice(['Amazon', 'Walmart', 'Target', 'Best Buy']),
                'status': random.choice(['completed', 'pending', 'failed']),
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal'])
            }
            events.append(event)
        return events
    
    def generate_system_metrics(self, count: int = 20) -> list:
        """Generate system metrics events"""
        events = []
        for _ in range(count):
            event = {
                'id': f"metric_{random.randint(100000, 999999)}",
                'service': random.choice(['api', 'database', 'cache', 'queue']),
                'cpu_usage': round(random.uniform(10, 90), 2),
                'memory_usage': round(random.uniform(20, 80), 2),
                'request_count': random.randint(100, 10000),
                'error_count': random.randint(0, 10),
                'latency_ms': random.randint(10, 500)
            }
            events.append(event)
        return events


if __name__ == "__main__":
    # Example usage
    producer = StreamingProducer()
    generator = EventGenerator()
    
    print("Starting Kafka producer...")
    
    try:
        # Produce user activity events
        user_events = generator.generate_user_activity(10)
        producer.produce_batch(user_events, 'user_activity')
        print(f"Produced {len(user_events)} user activity events")
        
        # Produce transaction events
        txn_events = generator.generate_transaction(5)
        producer.produce_batch(txn_events, 'transactions')
        print(f"Produced {len(txn_events)} transaction events")
        
        # Produce system metrics
        metric_events = generator.generate_system_metrics(20)
        producer.produce_batch(metric_events, 'metrics')
        print(f"Produced {len(metric_events)} metric events")
        
        time.sleep(2)  # Wait for messages to be sent
        
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()
        print("Producer closed")
