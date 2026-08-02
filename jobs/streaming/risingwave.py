# RisingWave integration for real-time-data-lakehouse

import psycopg2
from psycopg2 import sql
import json

class RisingWaveManager:
    def __init__(self, host='localhost', port=4566, database='dev', user='root'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.connection = None
    
    def connect(self):
        """Connect to RisingWave."""
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user
        )
        return self.connection
    
    def execute_sql(self, query):
        """Execute SQL query."""
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        return cursor
    
    def create_source(self, source_name, source_type, config):
        """Create a source in RisingWave."""
        if source_type == 'kafka':
            query = f"""
            CREATE SOURCE {source_name} WITH (
                connector = 'kafka',
                kafka.topic = '{config['topic']}',
                kafka.brokers = '{config['brokers']}',
                kafka.group.id = '{config['group_id']}',
                format = 'json'
            )
            """
        elif source_type == 'postgres':
            query = f"""
            CREATE SOURCE {source_name} WITH (
                connector = 'postgres-cdc',
                database.name = '{config['database']}',
                hostname = '{config['host']}',
                port = {config['port']},
                username = '{config['username']}',
                password = '{config['password']}',
                schema.name = '{config['schema']}',
                table.name = '{config['table']}'
            )
            """
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        return self.execute_sql(query)
    
    def create_materialized_view(self, mv_name, query):
        """Create a materialized view."""
        sql_query = f"""
        CREATE MATERIALIZED VIEW {mv_name} AS
        {query}
        """
        return self.execute_sql(sql_query)
    
    def create_index(self, index_name, table, columns):
        """Create an index on a materialized view."""
        columns_str = ', '.join(columns)
        query = f"""
        CREATE INDEX {index_name}
        ON {table} ({columns_str})
        """
        return self.execute_sql(query)
    
    def create_sink(self, sink_name, sink_type, config):
        """Create a sink in RisingWave."""
        if sink_type == 'kafka':
            query = f"""
            CREATE SINK {sink_name} FROM your_mv
            WITH (
                connector = 'kafka',
                kafka.topic = '{config['topic']}',
                kafka.brokers = '{config['brokers']}',
                format = 'json'
            )
            """
        elif sink_type == 'postgres':
            query = f"""
            CREATE SINK {sink_name} FROM your_mv
            WITH (
                connector = 'jdbc',
                jdbc.url = '{config['url']}',
                jdbc.table = '{config['table']}',
                username = '{config['username']}',
                password = '{config['password']}'
            )
            """
        else:
            raise ValueError(f"Unknown sink type: {sink_type}")
        
        return self.execute_sql(query)
    
    def create_table(self, table_name, schema):
        """Create a table."""
        columns = ', '.join([f'{name} {dtype}' for name, dtype in schema.items()])
        query = f"""
        CREATE TABLE {table_name} (
            {columns}
        )
        """
        return self.execute_sql(query)
    
    def insert_data(self, table_name, data):
        """Insert data into table."""
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({values})
        """
        return self.execute_sql(query)
    
    def query_data(self, query):
        """Query data from RisingWave."""
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    
    def drop_materialized_view(self, mv_name):
        """Drop a materialized view."""
        query = f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"
        return self.execute_sql(query)
    
    def drop_source(self, source_name):
        """Drop a source."""
        query = f"DROP SOURCE IF EXISTS {source_name}"
        return self.execute_sql(query)
    
    def drop_sink(self, sink_name):
        """Drop a sink."""
        query = f"DROP SINK IF EXISTS {sink_name}"
        return self.execute_sql(query)
    
    def flush_mv(self, mv_name):
        """Flush a materialized view."""
        query = f"FLUSH {mv_name}"
        return self.execute_sql(query)
    
    def explain_query(self, query):
        """Explain a query."""
        cursor = self.connection.cursor()
        cursor.execute(f"EXPLAIN {query}")
        return cursor.fetchall()
    
    def get_mv_progress(self, mv_name):
        """Get materialized view progress."""
        query = f"""
        SELECT * FROM rw_progress
        WHERE mv_name = '{mv_name}'
        """
        return self.query_data(query)
    
    def create_database(self, database_name):
        """Create a database."""
        query = f"CREATE DATABASE {database_name}"
        return self.execute_sql(query)
    
    def use_database(self, database_name):
        """Use a specific database."""
        query = f"USE DATABASE {database_name}"
        return self.execute_sql(query)
    
    def create_schema(self, schema_name):
        """Create a schema."""
        query = f"CREATE SCHEMA {schema_name}"
        return self.execute_sql(query)
    
    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()

if __name__ == '__main__':
    manager = RisingWaveManager()
    manager.connect()
    print('Connected to RisingWave successfully')
    
    # Create a table
    schema = {
        'event_id': 'VARCHAR',
        'event_type': 'VARCHAR',
        'user_id': 'VARCHAR',
        'timestamp': 'BIGINT',
        'data': 'JSONB'
    }
    
    manager.create_table('events', schema)
    print('Table created successfully')
    
    manager.close()
