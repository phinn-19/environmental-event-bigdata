"""Conservative technical gate before an explicitly authorised HF publication.

Validates integrity plus existence of every scoped table. This is NOT a
scientific quality guarantee or a substitute for source-date coverage review.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.processing_hf_package import TABLES, validate


def check(package: Path) -> dict:
    package = package.resolve()
    validate(package)
    report = json.loads((package / "SCOPE_REPORT.json").read_text(encoding="utf-8"))
    selection = json.loads((package / "SELECTION.json").read_text(encoding="utf-8"))
    expected = {table for names in TABLES.values() for table in names}
    tables = report.get("tables", {})
    if not isinstance(tables, dict) or set(tables) != expected:
        raise ValueError("PUBLISH BLOCKED: incomplete six-table scope report")
    zero = []
    for table in sorted(expected):
        record = tables[table]
        value = record.get("rows") if isinstance(record, dict) else None
        if type(value) is not int or value < 0:
            raise ValueError(f"PUBLISH BLOCKED: invalid row count for {table}")
        if value == 0:
            zero.append(table)
    if zero:
        raise ValueError("PUBLISH BLOCKED: empty target-day tables: " + ", ".join(zero))
    # Context tables are non-empty, but they might not have ANY actual ERA5 match.
    for table in ("air_quality_context_hourly", "fire_hotspots_context"):
        matched = tables[table].get("weather_matched_rows")
        if type(matched) is not int or matched < 1 or matched > tables[table]["rows"]:
            raise ValueError(f"PUBLISH BLOCKED: missing/invalid weather matches for {table}")
    # Output days must match the requested selection, rather than a misleading label.
    day_map = {
        "air_quality_hourly": "aq_day",
        "air_quality_baseline": "aq_day",
        "air_quality_context_hourly": "aq_day",
        "fire_hotspots": "firms_day",
        "fire_hotspots_context": "firms_day",
        "weather_hourly": "era5_day",
    }
    for table, key in day_map.items():
        if tables[table].get("day_utc") != selection.get(key):
            raise ValueError(f"PUBLISH BLOCKED: wrong target day in {table}")
    print("PUBLISH TECHNICAL GATE: PASS; all six target-day tables are nonempty")
    for table in ("air_quality_context_hourly", "fire_hotspots_context"):
        matched = tables[table]["weather_matched_rows"]
        total = tables[table]["rows"]
        rate = matched / total
        print(f"CONTEXT COVERAGE {table}: {matched}/{total} ({rate:.1%})")
        if rate < 0.5:
            print(f"QC WARNING: low weather match rate for {table}; human review needed")
    for warning in report.get("qc_warnings", []):
        print("QC WARNING:", warning)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    check(args.package)


if __name__ == "__main__":
    main()
