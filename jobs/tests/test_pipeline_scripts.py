import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOBS_DIR = ROOT / "jobs"
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

from producers.event_producer import emit_events, write_bronze
from streaming.bronze_ingestion import build_session_mart, load_bronze


class LakehousePipelineTests(unittest.TestCase):
    def test_emit_events_adds_metadata(self) -> None:
        events = [
            {"event_name": "page_view", "user_id": "u1", "session_id": "s1"},
            {"event_name": "click", "user_id": "u1", "session_id": "s1"},
        ]
        emitted = emit_events(events)

        self.assertEqual(len(emitted), 2)
        self.assertTrue(all(event["event_version"] == "1.0" for event in emitted))
        self.assertTrue(all(event["source"] == "sample-producer" for event in emitted))

    def test_bronze_to_session_mart(self) -> None:
        events = [
            {"event_name": "page_view", "user_id": "u1", "session_id": "s1"},
            {"event_name": "click", "user_id": "u1", "session_id": "s1"},
            {"event_name": "page_view", "user_id": "u2", "session_id": "s2"},
        ]
        enriched = emit_events(events)

        with tempfile.TemporaryDirectory() as temp_dir:
            bronze_path = Path(temp_dir) / "bronze.ndjson"
            write_bronze(enriched, bronze_path)
            bronze_events = load_bronze(bronze_path)

        mart = build_session_mart(bronze_events)
        by_session = {row["session_id"]: row for row in mart}

        self.assertEqual(by_session["s1"]["events"], 2)
        self.assertEqual(by_session["s2"]["events"], 1)


if __name__ == "__main__":
    unittest.main()
