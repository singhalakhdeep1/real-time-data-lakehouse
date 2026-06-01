import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOBS_DIR = ROOT / "jobs"
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

from producers.event_producer import emit_events, load_events


def run() -> dict:
    source = ROOT / "jobs" / "producers" / "sample_events.json"
    events = load_events(source)

    iterations = 5000
    started = time.perf_counter()
    total = 0
    for _ in range(iterations):
        total += len(emit_events(events))
    duration = max(0.001, time.perf_counter() - started)

    return {
        "workload": "event_enrichment",
        "input_batch_size": len(events),
        "iterations": iterations,
        "events_processed": total,
        "duration_sec": round(duration, 2),
        "events_per_sec": round(total / duration, 2),
    }


if __name__ == "__main__":
    report = run()
    output = Path(__file__).resolve().parent / "loadtest_report.json"
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
