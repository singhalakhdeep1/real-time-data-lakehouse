import json
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    report_path = root / "ops" / "loadtest" / "loadtest_report.json"
    targets_path = root / "ops" / "perf" / "slo_targets.json"

    report = json.loads(report_path.read_text(encoding="utf-8"))
    targets = json.loads(targets_path.read_text(encoding="utf-8"))
    thresholds = targets["thresholds"]

    checks = {
        "events_per_sec": report.get("events_per_sec", 0) >= thresholds["events_per_sec_min"],
    }

    passed = all(checks.values())
    result = {
        "project": targets["project"],
        "passed": passed,
        "checks": checks,
        "report": report,
        "thresholds": thresholds,
    }

    out_path = root / "ops" / "perf" / "slo_validation_report.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
