# Databricks integration for real-time-data-lakehouse

from databricks import sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class DatabricksManager:
    def __init__(self, server_hostname, http_path, access_token):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.connection = None
        
        self.spark = SparkSession.builder \
            .appName('DatabricksLakehouse') \
            .config('spark.databricks.delta.formatCheck.enabled', 'false') \
            .getOrCreate()
    
    def connect(self):
        """Connect to Databricks SQL endpoint."""
        self.connection = sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        )
        return self.connection
    
    def execute_sql(self, query):
        """Execute SQL query on Databricks."""
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    
    def create_unity_catalog(self, catalog_name):
        """Create a Unity Catalog."""
        self.execute_sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name}')
    
    def create_schema(self, catalog, schema):
        """Create a schema in Unity Catalog."""
        self.execute_sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')
    
    def create_managed_table(self, catalog, schema, table, ddl):
        """Create a managed table in Unity Catalog."""
        self.execute_sql(f'CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} {ddl}')
    
    def create_delta_live_table(self, table_name, query):
        """Create a Delta Live Table."""
        dlt_query = f"""
        CREATE LIVE TABLE {table_name} AS
        {query}
        """
        return dlt_query
    
    def create_streaming_live_table(self, table_name, query):
        """Create a streaming Delta Live Table."""
        dlt_query = f"""
        CREATE STREAMING LIVE TABLE {table_name} AS
        {query}
        """
        return dlt_query
    
    def apply_changes_to(self, target, source, keys, except_columns=None):
        """Apply changes from source to target (CDC)."""
        except_clause = f'EXCEPT {except_columns}' if except_columns else ''
        
        query = f"""
        APPLY CHANGES INTO {target}
        FROM source
        KEYS {keys}
        {except_clause}
        """
        return query
    
    def write_to_delta(self, df, table_path, mode='append'):
        """Write DataFrame to Delta Lake on Databricks."""
        df.write.format('delta').mode(mode).save(table_path)
    
    def write_to_unity_catalog(self, df, catalog, schema, table, mode='append'):
        """Write DataFrame to Unity Catalog."""
        df.write.format('delta').mode(mode).saveAsTable(f'{catalog}.{schema}.{table}')
    
    def read_from_delta(self, table_path):
        """Read from Delta Lake on Databricks."""
        return self.spark.read.format('delta').load(table_path)
    
    def read_from_unity_catalog(self, catalog, schema, table):
        """Read from Unity Catalog."""
        return self.spark.read.table(f'{catalog}.{schema}.{table}')
    
    def create_vector_search_index(self, catalog, schema, table, column, index_name):
        """Create a vector search index."""
        self.execute_sql(f"""
        CREATE VECTOR INDEX {index_name}
        ON {catalog}.{schema}.{table} ({column})
        """)
    
    def create_mlflow_experiment(self, experiment_name):
        """Create an MLflow experiment."""
        import mlflow
        mlflow.set_experiment(experiment_name)
    
    def log_model(self, model, model_name, signature):
        """Log a model to MLflow."""
        import mlflow
        mlflow.sklearn.log_model(model, model_name, signature=signature)
    
    def register_model(self, model_uri, model_name):
        """Register a model in MLflow Model Registry."""
        import mlflow
        mlflow.register_model(model_uri, model_name)
    
    def create_job(self, job_name, notebook_path, schedule=None):
        """Create a Databricks job."""
        job_spec = {
            'name': job_name,
            'notebook_task': {
                'notebook_path': notebook_path
            }
        }
        
        if schedule:
            job_spec['schedule'] = {
                'quartz_cron_expression': schedule,
                'timezone_id': 'UTC'
            }
        
        return job_spec
    
    def create_cluster(self, cluster_name, spark_version, node_type_id, num_workers):
        """Create a Databricks cluster."""
        cluster_spec = {
            'cluster_name': cluster_name,
            'spark_version': spark_version,
            'node_type_id': node_type_id,
            'num_workers': num_workers,
            'autotermination_minutes': 30
        }
        return cluster_spec
    
    def create_pipeline(self, pipeline_name, configuration):
        """Create a Delta Live Tables pipeline."""
        pipeline_spec = {
            'name': pipeline_name,
            'configuration': configuration,
            'clusters': [
                {
                    'label': 'default',
                    'autoscale': {
                        'min_workers': 1,
                        'max_workers': 5
                    }
                }
            ]
        }
        return pipeline_spec
    
    def close(self):
        """Close the Databricks connection."""
        if self.connection:
            self.connection.close()

if __name__ == '__main__':
    manager = DatabricksManager(
        server_hostname='your-workspace.cloud.databricks.com',
        http_path='/sql/your-endpoint',
        access_token='your-token'
    )
    
    manager.connect()
    print('Connected to Databricks successfully')
