import json
from collections import defaultdict
from pathlib import Path


def load_bronze(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def build_session_mart(events: list[dict]) -> list[dict]:
    sessions = defaultdict(lambda: {"events": 0, "user_id": None, "session_id": None})
    for event in events:
        session_id = event.get("session_id", "unknown")
        item = sessions[session_id]
        item["events"] += 1
        item["user_id"] = event.get("user_id")
        item["session_id"] = session_id

    return sorted(sessions.values(), key=lambda row: row["session_id"])


def write_gold(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"session_mart": rows}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    bronze_file = base / "bronze_events.ndjson"
    output_file = base / "gold_session_mart.json"
    bronze_events = load_bronze(bronze_file)
    session_rows = build_session_mart(bronze_events)
    write_gold(session_rows, output_file)
    print(f"Built session mart with {len(session_rows)} sessions")
