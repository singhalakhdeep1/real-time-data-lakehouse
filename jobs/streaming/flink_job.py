# Apache Flink streaming job for real-time-data-lakehouse

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.types import Types
from pyflink.datastream.functions import MapFunction, FilterFunction
import json

class FlinkStreamingJob:
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(4)
        
        self.table_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=EnvironmentSettings.in_streaming_mode()
        )
    
    def create_kafka_source(self, topic, bootstrap_servers='localhost:9092'):
        """Create a Kafka source table."""
        self.table_env.execute_sql(f"""
            CREATE TABLE kafka_source (
                event_id STRING,
                event_type STRING,
                user_id STRING,
                timestamp BIGINT,
                data STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{bootstrap_servers}',
                'properties.group.id' = 'flink-consumer',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            )
        """)
    
    def create_delta_sink(self, table_path):
        """Create a Delta Lake sink table."""
        self.table_env.execute_sql(f"""
            CREATE TABLE delta_sink (
                event_id STRING,
                event_type STRING,
                user_id STRING,
                timestamp BIGINT,
                data STRING,
                processed_at TIMESTAMP(3)
            ) WITH (
                'connector' = 'delta',
                'table-path' = '{table_path}',
                'format' = 'json'
            )
        """)
    
    def create_iceberg_sink(self, catalog_name, database, table):
        """Create an Iceberg sink table."""
        self.table_env.execute_sql(f"""
            CREATE CATALOG {catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-type' = 'hadoop',
                'warehouse' = 's3://warehouse'
            )
        """)
        
        self.table_env.execute_sql(f"""
            CREATE TABLE {catalog_name}.{database}.{table} (
                event_id STRING,
                event_type STRING,
                user_id STRING,
                timestamp BIGINT,
                data STRING,
                processed_at TIMESTAMP(3)
            ) PARTITIONED BY (event_type) WITH (
                'format-version' = '2',
                'write.format.default' = 'parquet'
            )
        """)
    
    def process_stream(self):
        """Process the streaming data."""
        self.table_env.execute_sql("""
            INSERT INTO delta_sink
            SELECT 
                event_id,
                event_type,
                user_id,
                timestamp,
                data,
                CURRENT_TIMESTAMP AS processed_at
            FROM kafka_source
            WHERE event_type IN ('click', 'view', 'purchase')
        """)
    
    def create_aggregation(self):
        """Create a streaming aggregation."""
        self.table_env.execute_sql("""
            CREATE TABLE event_aggregates AS
            SELECT 
                TUMBLE(proctime, INTERVAL '1' MINUTE) AS window,
               .event_type,
                COUNT(*) AS event_count,
                COUNT(DISTINCT user_id) AS unique_users
            FROM kafka_source
            GROUP BY 
                TUMBLE(proctime, INTERVAL '1' MINUTE),
                event_type
        """)
    
    def create_join(self):
        """Create a stream-stream join."""
        self.table_env.execute_sql("""
            CREATE TABLE user_profiles (
                user_id STRING,
                user_segment STRING,
                registration_date DATE
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://localhost:5432/users',
                'table-name' = 'profiles'
            )
        """)
        
        self.table_env.execute_sql("""
            CREATE TABLE enriched_events AS
            SELECT 
                k.event_id,
                k.event_type,
                k.user_id,
                u.user_segment,
                k.timestamp,
                k.data
            FROM kafka_source k
            LEFT JOIN user_profiles u
            ON k.user_id = u.user_id
        """)
    
    def run(self):
        """Execute the Flink job."""
        self.process_stream()
        self.env.execute('Real-time Data Lakehouse Job')

class EventFilter(FilterFunction):
    def filter(self, value):
        event = json.loads(value)
        return event.get('event_type') in ['click', 'view', 'purchase']

class EventMapper(MapFunction):
    def map(self, value):
        event = json.loads(value)
        return {
            'event_id': event.get('id'),
            'event_type': event.get('type'),
            'user_id': event.get('user_id'),
            'timestamp': event.get('timestamp'),
            'data': json.dumps(event.get('data', {}))
        }

if __name__ == '__main__':
    job = FlinkStreamingJob()
    job.create_kafka_source('events')
    job.create_delta_sink('/tmp/delta/events')
    job.run()
