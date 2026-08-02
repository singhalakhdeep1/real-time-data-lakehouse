# Apache Iceberg integration for real-time-data-lakehouse

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class IcebergManager:
    def __init__(self, app_name='IcebergLakehouse'):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
            .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
            .config('spark.sql.catalog.local.type', 'hadoop') \
            .config('spark.sql.catalog.local.warehouse', 's3://warehouse') \
            .config('spark.sql.defaultCatalog', 'local') \
            .getOrCreate()
    
    def create_catalog(self, name, catalog_type='hadoop', warehouse='s3://warehouse'):
        """Create an Iceberg catalog."""
        self.spark.conf.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
        self.spark.conf.set(f'spark.sql.catalog.{name}.type', catalog_type)
        self.spark.conf.set(f'spark.sql.catalog.{name}.warehouse', warehouse)
    
    def create_database(self, catalog, database):
        """Create a database in the catalog."""
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {catalog}.{database}')
    
    def create_table(self, catalog, database, table, schema, partition_by=None):
        """Create an Iceberg table."""
        partition_clause = f'PARTITIONED BY ({partition_by})' if partition_by else ''
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
                {', '.join([f'{name} {dtype}' for name, dtype in schema])}
            )
            {partition_clause}
            USING iceberg
        """)
    
    def write_stream(self, df, catalog, database, table, checkpoint_path):
        """Write streaming data to Iceberg."""
        query = df.writeStream \
            .format('iceberg') \
            .outputMode('append') \
            .option('checkpointLocation', checkpoint_path) \
            .toTable(f'{catalog}.{database}.{table}')
        return query
    
    def append_data(self, df, catalog, database, table):
        """Append data to Iceberg table."""
        df.writeTo(f'{catalog}.{database}.{table}').append()
    
    def overwrite_data(self, df, catalog, database, table):
        """Overwrite data in Iceberg table."""
        df.writeTo(f'{catalog}.{database}.{table}').overwritePartitions()
    
    def merge_data(self, source_df, catalog, database, table, condition, update_cols=None):
        """Merge data into Iceberg table."""
        target_table = f'{catalog}.{database}.{table}'
        
        merge_stmt = f"""
            MERGE INTO {target_table} AS target
            USING source AS src
            ON {condition}
        """
        
        if update_cols:
            update_stmt = ', '.join([f'target.{col} = src.{col}' for col in update_cols])
            merge_stmt += f' WHEN MATCHED THEN UPDATE SET {update_stmt}'
        
        merge_stmt += ' WHEN NOT MATCHED THEN INSERT *'
        
        source_df.createOrReplaceTempView('source')
        self.spark.sql(merge_stmt)
    
    def time_travel(self, catalog, database, table, snapshot_id=None, timestamp=None):
        """Query Iceberg table at a specific snapshot."""
        table_ref = f'{catalog}.{database}.{table}'
        
        if snapshot_id:
            return self.spark.read.format('iceberg').option('snapshot-id', snapshot_id).load(table_ref)
        elif timestamp:
            return self.spark.read.format('iceberg').option('as-of-timestamp', timestamp).load(table_ref)
        else:
            return self.spark.read.format('iceberg').load(table_ref)
    
    def get_snapshots(self, catalog, database, table):
        """Get snapshot history of Iceberg table."""
        return self.spark.sql(f'SELECT * FROM {catalog}.{database}.{table}.snapshots')
    
    def rollback_to_snapshot(self, catalog, database, table, snapshot_id):
        """Rollback Iceberg table to a specific snapshot."""
        self.spark.sql(f"""
            CALL {catalog}.system.rollback_to_snapshot(
                '{database}.{table}',
                {snapshot_id}
            )
        """)
    
    def expire_snapshots(self, catalog, database, table, older_than):
        """Expire old snapshots."""
        self.spark.sql(f"""
            CALL {catalog}.system.expire_snapshots(
                '{database}.{table}',
                older_than => '{older_than}'
            )
        """)
    
    def rewrite_manifests(self, catalog, database, table):
        """Rewrite manifests for Iceberg table."""
        self.spark.sql(f"""
            CALL {catalog}.system.rewrite_manifests('{database}.{table}')
        """)
    
    def add_partition(self, catalog, database, table, partition_values):
        """Add a partition to Iceberg table."""
        self.spark.sql(f"""
            ALTER TABLE {catalog}.{database}.{table}
            ADD PARTITION ({partition_values})
        """)
    
    def drop_partition(self, catalog, database, table, partition_values):
        """Drop a partition from Iceberg table."""
        self.spark.sql(f"""
            ALTER TABLE {catalog}.{database}.{table}
            DROP PARTITION ({partition_values})
        """)
    
    def migrate_to_delta(self, catalog, database, table, delta_path):
        """Migrate Iceberg table to Delta Lake."""
        df = self.spark.read.format('iceberg').load(f'{catalog}.{database}.{table}')
        df.write.format('delta').mode('overwrite').save(delta_path)

if __name__ == '__main__':
    manager = IcebergManager()
    
    # Create catalog and database
    manager.create_catalog('local')
    manager.create_database('local', 'analytics')
    
    # Define schema
    schema = [
        ('event_id', 'STRING'),
        ('event_type', 'STRING'),
        ('user_id', 'STRING'),
        ('timestamp', 'BIGINT'),
        ('data', 'STRING')
    ]
    
    # Create table
    manager.create_table('local', 'analytics', 'events', schema, partition_by='event_type')
    print('Iceberg table created successfully')
