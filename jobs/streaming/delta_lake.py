# Delta Lake integration for real-time-data-lakehouse

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class DeltaLakeManager:
    def __init__(self, app_name='DataLakehouse'):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
            .getOrCreate()
    
    def create_table(self, table_path, schema):
        """Create a Delta Lake table."""
        df = self.spark.createDataFrame([], schema)
        df.write.format('delta').save(table_path)
        return DeltaTable.forPath(self.spark, table_path)
    
    def write_stream(self, df, table_path, checkpoint_path):
        """Write a streaming DataFrame to Delta Lake."""
        query = df.writeStream \
            .format('delta') \
            .outputMode('append') \
            .option('checkpointLocation', checkpoint_path) \
            .start(table_path)
        return query
    
    def upsert(self, table_path, source_df, condition):
        """Perform upsert (merge) operation."""
        target_table = DeltaTable.forPath(self.spark, table_path)
        
        target_table.alias('target') \
            .merge(
                source_df.alias('source'),
                condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    
    def delete_records(self, table_path, condition):
        """Delete records from Delta Lake table."""
        target_table = DeltaTable.forPath(self.spark, table_path)
        target_table.delete(condition)
    
    def time_travel(self, table_path, version=None, timestamp=None):
        """Query Delta Lake at a specific version or timestamp."""
        if version:
            return self.spark.read.format('delta').option('versionAsOf', version).load(table_path)
        elif timestamp:
            return self.spark.read.format('delta').option('timestampAsOf', timestamp).load(table_path)
        else:
            return self.spark.read.format('delta').load(table_path)
    
    def get_history(self, table_path, limit=10):
        """Get the commit history of a Delta Lake table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        return delta_table.history(limit)
    
    def vacuum(self, table_path, retention_hours=168):
        """Vacuum old files from Delta Lake table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.vacuum(retention_hours)
    
    def optimize(self, table_path):
        """Optimize Delta Lake table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.optimize().executeCompaction()
    
    def create_zorder(self, table_path, columns):
        """Z-order Delta Lake table by specified columns."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.optimize().executeZOrderBy(columns)
    
    def get_schema(self, table_path):
        """Get the schema of a Delta Lake table."""
        return self.spark.read.format('delta').load(table_path).schema
    
    def add_constraints(self, table_path, constraints):
        """Add constraints to Delta Lake table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        for constraint in constraints:
            delta_table.alter().addConstraint(constraint)
    
    def generate_manifest(self, table_path, output_path):
        """Generate manifest file for Delta Lake table."""
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.generate(output_path)
    
    def convert_to_iceberg(self, delta_path, iceberg_path):
        """Convert Delta Lake table to Iceberg."""
        df = self.spark.read.format('delta').load(delta_path)
        df.writeTo(f'iceberg.{iceberg_path}').using('parquet').create()

if __name__ == '__main__':
    manager = DeltaLakeManager()
    
    # Define schema
    schema = StructType([
        StructField('event_id', StringType(), True),
        StructField('event_type', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('timestamp', LongType(), True),
        StructField('data', StringType(), True)
    ])
    
    # Create table
    table = manager.create_table('/tmp/delta/events', schema)
    print('Delta Lake table created successfully')
