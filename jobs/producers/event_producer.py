import json
from pathlib import Path


def load_events(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def emit_events(events: list[dict]) -> list[dict]:
    emitted = []
    for event in events:
        enriched = {
            **event,
            "event_version": "1.0",
            "source": "sample-producer",
        }
        emitted.append(enriched)
    return emitted


def write_bronze(events: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")


if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    input_file = base / "sample_events.json"
    output_file = base.parent / "streaming" / "bronze_events.ndjson"
    events = load_events(input_file)
    emitted_events = emit_events(events)
    write_bronze(emitted_events, output_file)
    print(f"Emitted {len(emitted_events)} events to {output_file}")
