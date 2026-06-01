# Real-Time Data Lakehouse

Streaming data platform that ingests operational events, lands them in a raw lake, transforms them into analytics-ready models, and serves dashboards.

## Architecture

- Producers: web, backend, payment, and support events
- Bus: Kafka
- Raw storage: MinIO or S3 parquet bronze layer
- Stream processing: Spark Structured Streaming or Flink
- Orchestration: Airflow
- Transform: dbt
- Serving: ClickHouse
- BI: Superset or Metabase

## Layers

1. Bronze: immutable raw events
2. Silver: validated and enriched events
3. Gold: marts for funnels, retention, revenue, and ops

## First build milestone

1. Local Kafka + MinIO + ClickHouse + Airflow compose stack
2. Event producer simulator
3. Bronze ingestion job
4. dbt marts for sessions and funnels

## Implemented baseline

- Local infra compose for Kafka, MinIO, and ClickHouse
- Python event producer writing bronze NDJSON stream
- Bronze to gold transformation script producing session mart JSON
- Airflow DAG wiring producer and transform sequence
- dbt seed-style event model and growth funnel SQL mart
- Unit tests for producer enrichment and session-mart aggregation logic

## Run local pipeline scripts

```bash
cd jobs/producers
python event_producer.py

cd ../streaming
python bronze_ingestion.py
```

## One-command local demo

```bash
cd jobs
python run_local_demo.py
```

Generated artifacts:

- `jobs/streaming/bronze_events.ndjson`
- `jobs/streaming/gold_session_mart.json`
- `jobs/streaming/run_summary.json`

## Validate

```bash
cd jobs/tests
python -m unittest discover -s . -p "test_*.py"
```
