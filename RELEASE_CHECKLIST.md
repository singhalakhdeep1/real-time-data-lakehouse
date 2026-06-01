# Real-Time Data Lakehouse - Release Checklist

Release status: Production-demo ready
Last updated: 2026-06-01

## Acceptance Criteria

- [x] Producer flow implemented and runnable: `jobs/producers/event_producer.py`
- [x] Bronze to gold transform implemented and runnable: `jobs/streaming/bronze_ingestion.py`
- [x] One-command demo run works: `python jobs/run_local_demo.py`
- [x] Demo artifacts are generated:
  - `jobs/streaming/bronze_events.ndjson`
  - `jobs/streaming/gold_session_mart.json`
  - `jobs/streaming/run_summary.json`
- [x] Unit tests pass: `python -m unittest discover -s jobs/tests -p "test_*.py"`
- [x] Airflow DAG scaffold exists for orchestration flow
- [x] CI workflow exists and runs demo + tests: `.github/workflows/ci.yml`

## Production-Demo Sign-Off

- [x] Ready for recruiter demo
- [x] Ready for portfolio showcase
- [x] Ready for interview walk-through

## Notes

Implementation is intentionally local-first and file-based to keep demo setup fast and dependency-light.
