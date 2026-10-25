"""Load bronze NDJSON events into DuckDB medallion lakehouse."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb


def load_bronze(db_path: Path, bronze_dir: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    total = 0
    for ndjson_file in sorted(bronze_dir.glob("*.ndjson")):
        rows = [
            json.loads(line)
            for line in ndjson_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not rows:
            continue
        posix = ndjson_file.as_posix()
        table = ndjson_file.stem
        con.execute(f"DROP TABLE IF EXISTS bronze.{table}")
        con.execute(
            f"CREATE TABLE bronze.{table} AS "
            f"SELECT * FROM read_json_auto('{posix}', auto_detect=true)"
        )
        count = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
        print(f"  Loaded {count:>5} rows → bronze.{table}")
        total += count

    con.close()
    return total


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    load_bronze(
        db_path=root / "data" / "warehouse.duckdb",
        bronze_dir=root / "data" / "bronze",
    )
    print("Bronze load complete.")
