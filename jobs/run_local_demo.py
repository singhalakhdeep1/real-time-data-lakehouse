import json
from pathlib import Path

from producers.event_producer import emit_events, load_events, write_bronze
from streaming.bronze_ingestion import build_session_mart, write_gold


def run_demo() -> dict:
    base = Path(__file__).resolve().parent
    producer_dir = base / "producers"
    streaming_dir = base / "streaming"

    events = load_events(producer_dir / "sample_events.json")
    enriched = emit_events(events)
    bronze_path = streaming_dir / "bronze_events.ndjson"
    write_bronze(enriched, bronze_path)

    session_rows = build_session_mart(enriched)
    gold_path = streaming_dir / "gold_session_mart.json"
    write_gold(session_rows, gold_path)

    summary = {
        "input_events": len(events),
        "enriched_events": len(enriched),
        "sessions_built": len(session_rows),
        "bronze_output": str(bronze_path),
        "gold_output": str(gold_path),
    }
    summary_path = streaming_dir / "run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run_demo(), indent=2))
