# Security Policy — Real-Time Data Lakehouse

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 1.x     | ✅ Yes    |
| < 1.0   | ❌ No     |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report security issues to: **security@alakhdeepsingh.dev**

Include in your report:

- Description of the vulnerability and its impact
- Steps to reproduce or proof-of-concept
- Affected component (producer, bronze ingestion, gold transform, CI pipeline)
- Suggested remediation if known

We acknowledge reports within **48 hours** and aim to provide a resolution timeline within **7 days**.

## Security Controls

### Dependency Management

- This project uses Python 3.12 standard library only (zero third-party runtime dependencies)
- `ops/security/dependency_scan.py` runs on every CI push — confirms stdlib-only posture
- SBOM is maintained at `ops/security/sbom.txt` (SPDX 2.3 format)

### Data Security

- Bronze events are written as NDJSON files; no encryption at rest in the demo setup
- In production, all S3/GCS buckets should have server-side encryption (AES-256) enabled
- Event payloads must not contain PII without field-level masking applied at the producer

### Pipeline Security

- The event producer adds `event_version` and `source` metadata to every record — enabling downstream lineage tracking
- Gold-layer session marts are computed from bronze without re-reading raw source systems
- No credentials or connection strings appear in any pipeline source file

### Container Security

- Worker images are built from `python:3.12-slim`
- No secrets baked into images; runtime secrets injected via Kubernetes secrets

### Kubernetes Security

- Worker pods run as non-root (`runAsNonRoot: true`)
- Resource limits prevent runaway jobs from exhausting cluster resources

## Known Limitations (Demo Scope)

This project simulates Kafka/Flink/Delta Lake patterns using local files for portability. In a production deployment:

- Replace file-based bronze store with Apache Kafka + Delta Lake / Apache Iceberg
- Add column-level encryption for sensitive event fields
- Integrate with a data catalog (Apache Atlas, DataHub) for lineage

## Security Changelog

| Date       | Change                                        |
| ---------- | --------------------------------------------- |
| 2025-06-01 | Initial security policy published             |
| 2025-06-01 | Dependency scan and SBOM added to CI pipeline |
