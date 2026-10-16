"""Read-only coverage and preliminary quality report for OpenAQ hourly Raw.

This module does NOT normalize measurements, calculate a baseline, or write
Silver/Curated tables. An hourly observation remains in Raw even if flagged.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
HOURS_PER_DAY = 24


def _load_records(raw_path: Path) -> list[dict]:
    """Read one OpenAQ page; do not change the provider payload."""
    payload = json.loads(raw_path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
        raise ValueError("OpenAQ Raw must contain a results array")
    return payload["results"]


def _inspect_records(records: list[dict], expected_day: date) -> dict:
    """Quality counts plus hourly diagnostics; accepts one OR multiple pages."""
    hours = []
    usable_by_hour = [0] * HOURS_PER_DAY
    null_count = flagged_count = flagged_with_value = usable_count = 0

    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Invalid observation")
        period = record.get("period") or {}
        if not isinstance(period, dict):
            raise ValueError("Invalid period object")
        from_info = period.get("datetimeFrom") or {}
        if not isinstance(from_info, dict):
            raise ValueError("Invalid datetimeFrom object")
        timestamp = from_info.get("utc")
        if not isinstance(timestamp, str):
            raise ValueError("Missing hourly UTC timestamp")
        try:
            moment = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Invalid UTC timestamp") from exc
        if (
            moment.utcoffset() != timedelta(0)
            or moment.date() != expected_day
            or moment.minute != 0
            or moment.second != 0
            or moment.microsecond != 0
        ):
            raise ValueError(f"Invalid hourly timestamp: {timestamp}")
        hours.append(moment.hour)

        value = record.get("value")
        flag_info = record.get("flagInfo") or {}
        if not isinstance(flag_info, dict):
            raise ValueError("Invalid flagInfo object")
        flagged = flag_info.get("hasFlags") is True
        if value is None:
            null_count += 1
        if flagged:
            flagged_count += 1
        if flagged and value is not None:
            flagged_with_value += 1
        if value is not None and not flagged:
            usable_count += 1
            usable_by_hour[moment.hour] += 1

    unique_hours = set(hours)
    return {
        "rows": len(records),
        "unique_hours": len(unique_hours),
        "missing_hours": sorted(set(range(HOURS_PER_DAY)) - unique_hours),
        "duplicate_rows": len(hours) - len(unique_hours),
        "null": null_count,
        "flagged": flagged_count,
        "flagged_with_value": flagged_with_value,
        "usable": usable_count,
        "usable_by_hour": usable_by_hour,
    }


def inspect_openaq_day(raw_path: Path, expected_day: date) -> dict:
    """Keep original public helper for the six existing unit tests."""
    return _inspect_records(_load_records(raw_path), expected_day)


def load_min_valid_samples(config_path: Path) -> int:
    """Read the threshold from the project config (no hard-coded 10)."""
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    minimum = config["algorithms"]["air_quality"]["baseline"]["min_valid_samples"]
    if type(minimum) is not int or minimum < 1:
        raise ValueError("min_valid_samples must be a positive integer")
    return minimum


def _safe_raw_path(root: Path, relative: str) -> Path:
    """Reject absolute/out-of-repository manifest paths."""
    if not isinstance(relative, str) or not relative.strip():
        raise ValueError("Missing Raw path")
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError("Unsafe Raw path")
    resolved_root = root.resolve()
    path = (resolved_root / rel).resolve()
    if not path.is_relative_to(resolved_root / "data" / "raw"):
        raise ValueError("Raw path is not under data/raw")
    return path


def _read_verified_page(manifest: dict, root: Path) -> list[dict]:
    if manifest.get("status") != "success":
        raise ValueError("Manifest status not success")
    raw_path = _safe_raw_path(root, manifest.get("file"))
    if not raw_path.is_file():
        raise ValueError("Raw file missing")
    expected_hash = manifest.get("sha256")
    if not isinstance(expected_hash, str) or len(expected_hash) != 64:
        raise ValueError("Missing or invalid SHA256")
    with raw_path.open("rb") as stream:
        actual_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual_hash.lower() != expected_hash.lower():
        raise ValueError("SHA256 mismatch")
    records = _load_records(raw_path)
    row_count = manifest.get("row_count")
    if type(row_count) is not int or row_count != len(records):
        raise ValueError("row_count mismatch")
    return records


def build_coverage_report(
    sensor_id: int,
    start: date,
    end: date,
    *,
    root: Path = ROOT,
    min_valid_samples: int = 10,
) -> dict:
    """Build a report for [start, end), combining all pages of each UTC day."""
    if end <= start:
        raise ValueError("--end must be after --start")
    if type(min_valid_samples) is not int or min_valid_samples < 1:
        raise ValueError("min_valid_samples must be a positive integer")

    manifest_dir = root / "data/raw/openaq/manifests" / f"sensor={sensor_id}"
    if not manifest_dir.is_dir():
        raise FileNotFoundError(f"Manifest directory not found: {manifest_dir}")

    by_day: dict[str, list[dict]] = {}
    problems = []
    for path in sorted(manifest_dir.rglob("*.manifest.json")):
        try:
            manifest = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(manifest, dict):
                raise ValueError("Manifest must be a JSON object")
            if manifest.get("source") != "openaq" or manifest.get("acquisition_mode") != "live":
                continue
            day = str(manifest.get("start_utc") or "")[:10]
            if start.isoformat() <= day < end.isoformat():
                by_day.setdefault(day, []).append(manifest)
        except (OSError, ValueError, TypeError):
            problems.append(f"Unreadable manifest: {path.relative_to(root)}")

    daily = []
    warnings = []
    hourly_totals = [0] * HOURS_PER_DAY
    quality_totals = {key: 0 for key in ("null", "flagged", "flagged_with_value", "usable")}
    verified_days = total_rows = 0
    day = start
    while day < end:
        day_str = day.isoformat()
        pages = by_day.get(day_str, [])
        day_errors = []
        records = []
        if not pages:
            day_errors.append("missing manifest")
        for manifest in pages:
            try:
                records.extend(_read_verified_page(manifest, root))
            except (ValueError, OSError, TypeError, json.JSONDecodeError) as exc:
                day_errors.append(str(exc))

        stats = None
        if not day_errors:
            try:
                stats = _inspect_records(records, day)
                if stats["unique_hours"] != HOURS_PER_DAY or stats["duplicate_rows"]:
                    day_errors.append("incomplete or duplicate UTC hours")
            except (ValueError, TypeError) as exc:
                day_errors.append(str(exc))

        if day_errors:
            problems.extend(f"{day_str}: {error}" for error in day_errors)
        else:
            verified_days += 1
            total_rows += stats["rows"]
            for key in quality_totals:
                quality_totals[key] += stats[key]
            for hour in range(HOURS_PER_DAY):
                hourly_totals[hour] += stats["usable_by_hour"][hour]
            if stats["usable"] == 0:
                warnings.append(f"{day_str}: NO_USABLE_MEASUREMENTS")
            elif stats["usable"] < HOURS_PER_DAY:
                warnings.append(f"{day_str}: PARTIAL_USABLE ({stats['usable']}/24)")

        daily.append({
            "date": day_str,
            "status": "verified" if not day_errors else "problem",
            "pages": len(pages),
            "rows": stats["rows"] if stats and not day_errors else None,
            "unique_hours": stats["unique_hours"] if stats and not day_errors else None,
            "missing_hours": stats["missing_hours"] if stats and not day_errors else None,
            "duplicate_rows": stats["duplicate_rows"] if stats and not day_errors else None,
            "null": stats["null"] if stats and not day_errors else None,
            "flagged": stats["flagged"] if stats and not day_errors else None,
            "flagged_with_value": stats["flagged_with_value"] if stats and not day_errors else None,
            "usable": stats["usable"] if stats and not day_errors else None,
            "problems": day_errors,
        })
        day += timedelta(days=1)

    hourly_samples = [
        {"hour_utc": f"{h:02d}:00", "usable_samples": count,
         "meets_minimum": count >= min_valid_samples}
        for h, count in enumerate(hourly_totals)
    ]
    return {
        "source": "openaq",
        "sensor_id": str(sensor_id),
        "start_utc": f"{start.isoformat()}T00:00:00Z",
        "end_utc": f"{end.isoformat()}T00:00:00Z",
        "expected_days": (end - start).days,
        "verified_days": verified_days,
        "manifest_raw_rows": total_rows,
        "quality_totals": quality_totals,
        "usable_rate_percent": round(quality_totals["usable"] * 100 / total_rows, 2) if total_rows else 0.0,
        "quality_warnings": warnings,
        "technical_problems": problems,
        "min_valid_samples": min_valid_samples,
        "hourly_samples": hourly_samples,
        "hours_meeting_minimum": sum(x["meets_minimum"] for x in hourly_samples),
        "daily": daily,
    }


def export_coverage_report(report: dict, output_dir: Path) -> tuple[Path, Path]:
    """Write one machine-readable JSON and one per-day CSV; never modify Raw."""
    output_dir.mkdir(parents=True, exist_ok=True)
    start_day = report["start_utc"][:10]
    end_day = report["end_utc"][:10]
    base = f"openaq_sensor_{report['sensor_id']}_{start_day}_{end_day}"
    json_path = output_dir / f"{base}.json"
    csv_path = output_dir / f"{base}_daily.csv"

    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    fields = ["date", "status", "pages", "rows", "unique_hours", "missing_hours",
              "duplicate_rows", "null", "flagged", "flagged_with_value", "usable", "problems"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for day in report["daily"]:
            row = dict(day)
            row["missing_hours"] = ";".join(str(h) for h in (row["missing_hours"] or []))
            row["problems"] = "; ".join(row["problems"])
            writer.writerow({key: row.get(key) for key in fields})
    return json_path, csv_path


def check_openaq_coverage(
    sensor_id: int, start: date, end: date, *, root: Path = ROOT,
    min_valid_samples: int = 10, output_dir: Path | None = None,
) -> int:
    """CLI-friendly wrapper; preserves original integer exit-code convention."""
    report = build_coverage_report(sensor_id, start, end, root=root,
                                   min_valid_samples=min_valid_samples)
    for day in report["daily"]:
        if day["status"] == "verified":
            print(f"{day['date']}: rows={day['rows']}, null={day['null']}, "
                  f"flagged={day['flagged']}, usable={day['usable']}")
    print("=== OpenAQ Raw Coverage ===")
    print("Sensor:", sensor_id)
    print("Window:", start, "to", end, "(end exclusive)")
    print("Expected days:", report["expected_days"])
    print("Verified days:", report["verified_days"])
    print("Manifest raw rows:", report["manifest_raw_rows"])
    print("Problems:", len(report["technical_problems"]))
    for p in report["technical_problems"]:
        print(" -", p)
    quality = report["quality_totals"]
    print("\n=== OpenAQ Data Quality ===")
    print("Total null:", quality["null"])
    print("Total flagged:", quality["flagged"])
    print("Flagged with value:", quality["flagged_with_value"])
    print("Total usable:", quality["usable"])
    print(f"Usable rate: {report['usable_rate_percent']:.2f}%")
    print("Quality warnings:", len(report["quality_warnings"]))
    for w in report["quality_warnings"]:
        print(" !", w)
    print("\n=== Historical Sample Counts by UTC Hour ===")
    for hour in report["hourly_samples"]:
        print(f"{hour['hour_utc']} UTC: usable_samples={hour['usable_samples']}, "
              f"meets_minimum={hour['meets_minimum']}")
    print("Hours meeting preliminary minimum:", f"{report['hours_meeting_minimum']}/24")
    if output_dir is not None:
        json_path, csv_path = export_coverage_report(report, output_dir)
        print("JSON report:", json_path)
        print("CSV report:", csv_path)
    return 0 if not report["technical_problems"] else 2


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenAQ Raw coverage and quality report")
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--start", required=True, help="UTC start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="UTC exclusive end date YYYY-MM-DD")
    parser.add_argument("--config", default="configs/project.yaml")
    parser.add_argument("--export", action="store_true", help="Save JSON and daily CSV")
    parser.add_argument("--output-dir", default="data/reports/ingestion/openaq")
    args = parser.parse_args()
    try:
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        minimum = load_min_valid_samples(config_path)
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = ROOT / output_dir
        return check_openaq_coverage(args.sensor_id, start, end,
                                     min_valid_samples=minimum,
                                     output_dir=output_dir if args.export else None)
    except (ValueError, KeyError, TypeError, OSError) as exc:
        parser.exit(2, f"COVERAGE ERROR: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
