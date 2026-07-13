"""
Kafka Streaming Consumer for Real-time Analytics
Consumes streaming data from Kafka and performs real-time analytics
"""

import json
import time
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaConsumer
from collections import defaultdict
import statistics


class StreamingConsumer:
    """Kafka consumer for real-time data processing"""
    
    def __init__(self, bootstrap_servers='localhost:9092', group_id='analytics_group'):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.topics = ['raw_events', 'transactions_stream', 'user_activity_stream', 'system_metrics']
        self.consumer.subscribe(self.topics)
        
        # Real-time analytics state
        self.metrics = {
            'events_processed': 0,
            'events_by_type': defaultdict(int),
            'active_users': set(),
            'transaction_totals': defaultdict(float),
            'system_health': defaultdict(list)
        }
    
    def consume_events(self, max_messages: int = 100, timeout: float = 10.0):
        """Consume events from Kafka and perform real-time analytics"""
        messages_consumed = 0
        
        print(f"Starting consumer for topics: {self.topics}")
        
        for message in self.consumer:
            try:
                self._process_message(message)
                messages_consumed += 1
                
                if messages_consumed >= max_messages:
                    print(f"Reached max messages: {max_messages}")
                    break
                    
            except Exception as e:
                print(f"Error processing message: {e}")
        
        return self._get_analytics_summary()
    
    def _process_message(self, message):
        """Process a single message and update analytics"""
        event = message.value
        event_type = event.get('event_type', 'unknown')
        
        self.metrics['events_processed'] += 1
        self.metrics['events_by_type'][event_type] += 1
        
        # Process based on event type
        if event_type in ['page_view', 'click', 'signup']:
            self._process_user_activity(event)
        elif event_type == 'purchase':
            self._process_transaction(event)
        elif message.topic == 'system_metrics':
            self._process_system_metrics(event)
    
    def _process_user_activity(self, event: Dict[str, Any]):
        """Process user activity events"""
        user_id = event.get('user_id')
        if user_id:
            self.metrics['active_users'].add(user_id)
    
    def _process_transaction(self, event: Dict[str, Any]):
        """Process transaction events"""
        amount = event.get('amount', 0)
        currency = event.get('currency', 'USD')
        self.metrics['transaction_totals'][currency] += amount
    
    def _process_system_metrics(self, event: Dict[str, Any]):
        """Process system metrics events"""
        service = event.get('service', 'unknown')
        cpu_usage = event.get('cpu_usage', 0)
        memory_usage = event.get('memory_usage', 0)
        latency = event.get('latency_ms', 0)
        
        self.metrics['system_health'][service].append({
            'cpu': cpu_usage,
            'memory': memory_usage,
            'latency': latency,
            'timestamp': event.get('timestamp')
        })
    
    def _get_analytics_summary(self) -> Dict[str, Any]:
        """Generate real-time analytics summary"""
        # Calculate system health averages
        health_summary = {}
        for service, metrics in self.metrics['system_health'].items():
            if metrics:
                health_summary[service] = {
                    'avg_cpu': statistics.mean(m['cpu'] for m in metrics),
                    'avg_memory': statistics.mean(m['memory'] for m in metrics),
                    'avg_latency': statistics.mean(m['latency'] for m in metrics),
                    'sample_count': len(metrics)
                }
        
        return {
            'summary': {
                'total_events_processed': self.metrics['events_processed'],
                'unique_active_users': len(self.metrics['active_users']),
                'events_by_type': dict(self.metrics['events_by_type']),
                'total_transaction_volume': dict(self.metrics['transaction_totals'])
            },
            'system_health': health_summary,
            'timestamp': datetime.now().isoformat()
        }
    
    def close(self):
        """Close the consumer"""
        self.consumer.close()


class RealTimeAnalytics:
    """Real-time analytics engine for streaming data"""
    
    def __init__(self):
        self.window_size = 60  # 60 second window
        self.event_windows = defaultdict(list)
        self.aggregations = {
            'count': 0,
            'sum': defaultdict(float),
            'avg': defaultdict(list),
            'min': defaultdict(list),
            'max': defaultdict(list)
        }
    
    def process_event(self, event: Dict[str, Any]):
        """Process event in real-time with windowing"""
        event_time = datetime.fromisoformat(event.get('timestamp', datetime.now().isoformat()))
        event_type = event.get('event_type', 'unknown')
        
        # Add to window
        self.event_windows[event_type].append({
            'event': event,
            'timestamp': event_time
        })
        
        # Clean old events outside window
        self._clean_windows()
        
        # Update aggregations
        self._update_aggregations(event, event_type)
    
    def _clean_windows(self):
        """Remove events outside the time window"""
        cutoff = datetime.now() - timedelta(seconds=self.window_size)
        
        for event_type in self.event_windows:
            self.event_windows[event_type] = [
                e for e in self.event_windows[event_type]
                if e['timestamp'] > cutoff
            ]
    
    def _update_aggregations(self, event: Dict[str, Any], event_type: str):
        """Update real-time aggregations"""
        self.aggregations['count'] += 1
        
        # Sum numeric fields
        for key, value in event.items():
            if isinstance(value, (int, float)):
                self.aggregations['sum'][f"{event_type}_{key}"] += value
                self.aggregations['avg'][f"{event_type}_{key}"].append(value)
                self.aggregations['min'][f"{event_type}_{key}"].append(value)
                self.aggregations['max'][f"{event_type}_{key}"].append(value)
    
    def get_realtime_metrics(self) -> Dict[str, Any]:
        """Get current real-time metrics"""
        avg_metrics = {}
        min_metrics = {}
        max_metrics = {}
        
        for key, values in self.aggregations['avg'].items():
            if values:
                avg_metrics[key] = statistics.mean(values)
        
        for key, values in self.aggregations['min'].items():
            if values:
                min_metrics[key] = min(values)
        
        for key, values in self.aggregations['max'].items():
            if values:
                max_metrics[key] = max(values)
        
        return {
            'window_size_seconds': self.window_size,
            'events_in_window': {
                event_type: len(events)
                for event_type, events in self.event_windows.items()
            },
            'aggregations': {
                'total_events': self.aggregations['count'],
                'sums': dict(self.aggregations['sum']),
                'averages': avg_metrics,
                'minimums': min_metrics,
                'maximums': max_metrics
            },
            'timestamp': datetime.now().isoformat()
        }


from datetime import timedelta


if __name__ == "__main__":
    # Example usage
    consumer = StreamingConsumer()
    
    print("Starting Kafka consumer...")
    
    try:
        summary = consumer.consume_events(max_messages=50, timeout=30.0)
        print("\nReal-time Analytics Summary:")
        print(json.dumps(summary, indent=2))
        
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed")
