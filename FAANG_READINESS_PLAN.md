# Real-Time Data Lakehouse - FAANG Readiness Plan

Status: FAANG hardening phase 1 complete (demo to production transition plan implemented)
Last updated: 2026-06-01

## Target SLOs

- Pipeline availability: 99.9%
- End-to-end ingestion latency p95: <= 60 seconds
- Data freshness for gold marts: <= 5 minutes
- Failed batch ratio: < 1%
- Recovery objective (RTO): <= 45 minutes
- Data loss objective (RPO): <= 5 minutes

## Capacity Targets

- Ingest throughput: 150k events per minute
- Peak throughput: 400k events per minute for 15 minutes
- Session mart generation: <= 2 minutes for 1 million events

## Security Controls

- Producer authentication and topic ACLs
- Encryption at rest for raw and transformed data
- Encryption in transit for bus and storage endpoints
- PII classification and masking in analytics outputs
- Vulnerability scan for runtime images and dependencies

## Reliability and Operations

- Streaming lag and throughput dashboards
- Alerting on lag, dropped events, and transform failures
- Replay and backfill runbook for data recovery
- Synthetic probe for producer-to-gold pipeline flow

## Delivery Gates

- Local demo run required in CI
- Unit tests for producer and transformation logic required
- Data contract changes must include backward compatibility note

## Acceptance Criteria

- [x] Release checklist present and maintained
- [x] CI pipeline present and validates demo + tests
- [x] Run summary artifact in place
- [x] Explicit SLO, security, and reliability targets documented
- [x] Automated readiness audit script present
