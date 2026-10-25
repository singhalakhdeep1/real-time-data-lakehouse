"""Full medallion pipeline demo: generate → bronze → dbt silver/gold → KPI report."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"
DATA_DIR = ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
DB_PATH = DATA_DIR / "warehouse.duckdb"


def step_generate() -> int:
    print("\n=== Step 1: Generate streaming events (bronze NDJSON) ===")
    from streaming.event_generator import generate_events, write_bronze_ndjson
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    events = generate_events(n_users=80)
    out = BRONZE_DIR / "events.ndjson"
    write_bronze_ndjson(events, out)
    print(f"  Generated {len(events)} events → {out}")
    return len(events)


def step_load_bronze() -> int:
    print("\n=== Step 2: Load bronze layer into DuckDB ===")
    from streaming.lakehouse_writer import load_bronze
    return load_bronze(DB_PATH, BRONZE_DIR)


def step_dbt() -> int:
    print("\n=== Step 3: dbt run (silver staging + gold marts) ===")
    env = {**os.environ, "DBT_DUCKDB_PATH": str(DB_PATH)}
    rc = subprocess.run(
        ["dbt", "run", "--profiles-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        env=env,
    ).returncode
    return rc


def step_kpi_report() -> dict:
    print("\n=== Step 4: KPI report from gold marts ===")
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)

        funnel = con.execute(
            "SELECT total_sessions, total_users, total_events, "
            "sessions_with_payment, overall_conversion_pct "
            "FROM mart_session_funnel LIMIT 1"
        ).fetchone()

        orders = con.execute(
            "SELECT total_orders, total_revenue_usd, avg_order_value_usd, "
            "shipped_rate_pct, delivered_rate_pct "
            "FROM mart_order_metrics LIMIT 1"
        ).fetchone()

        growth = con.execute(
            "SELECT signup_completed, checkout_started, payment_succeeded, "
            "signup_to_payment_rate "
            "FROM mart_growth_funnel LIMIT 1"
        ).fetchone()

        con.close()

        report = {
            "session_funnel": {
                "total_sessions": funnel[0],
                "total_users": funnel[1],
                "total_events": funnel[2],
                "converting_sessions": funnel[3],
                "overall_conversion_pct": funnel[4],
            },
            "order_metrics": {
                "total_orders": orders[0],
                "total_revenue_usd": orders[1],
                "avg_order_value_usd": orders[2],
                "shipped_rate_pct": orders[3],
                "delivered_rate_pct": orders[4],
            },
            "growth_funnel": {
                "signups": growth[0],
                "checkout_started": growth[1],
                "payments_succeeded": growth[2],
                "signup_to_payment_rate": growth[3],
            },
        }
        print(json.dumps(report, indent=2))
        return report
    except Exception as exc:
        print(f"KPI report skipped (dbt may not have run): {exc}")
        return {}


def run_demo() -> dict:
    n_events = step_generate()
    n_loaded = step_load_bronze()
    dbt_rc = step_dbt()
    if dbt_rc != 0:
        print("WARNING: dbt run exited with errors — KPI report may be incomplete.")
    kpis = step_kpi_report()

    summary = {
        "events_generated": n_events,
        "bronze_rows_loaded": n_loaded,
        "dbt_exit_code": dbt_rc,
        "kpis": kpis,
    }
    print("\n=== Demo complete ===")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    run_demo()
