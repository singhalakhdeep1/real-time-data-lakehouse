from datetime import datetime
from pathlib import Path
from subprocess import run

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_producer() -> None:
    root = Path(__file__).resolve().parents[3]
    script = root / "jobs" / "producers" / "event_producer.py"
    run(["python", str(script)], check=True)


def run_bronze_to_gold() -> None:
    root = Path(__file__).resolve().parents[3]
    script = root / "jobs" / "streaming" / "bronze_ingestion.py"
    run(["python", str(script)], check=True)


with DAG(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lakehouse", "demo"],
) as dag:
    produce = PythonOperator(task_id="produce_events", python_callable=run_producer)
    transform = PythonOperator(task_id="bronze_to_gold", python_callable=run_bronze_to_gold)
    produce >> transform
