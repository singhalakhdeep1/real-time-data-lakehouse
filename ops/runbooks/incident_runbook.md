# Incident Runbook - Real-Time Data Lakehouse

## Triggers

- Ingestion lag SLO breach
- Pipeline failure rate increase
- Gold mart generation delays

## Immediate Steps

1. Identify impacted stream topics and downstream marts.
2. Verify producer health and transform job status.
3. Pause non-critical jobs to free capacity.
4. Trigger replay/backfill for missing windows if needed.

## Recovery Steps

1. Restore producer-to-gold path stability.
2. Re-run local demo path `jobs/run_local_demo.py` as baseline check.
3. Validate output artifacts and freshness metrics.

## Post-Incident

1. Document failed stage and corrective action.
2. Add or tune lag alerts and capacity thresholds.
3. Update backfill playbook with lessons learned.
