"""Select bounded, checksum-verified HF Raw for the existing six Spark modules.

This is a one-run staging utility, NOT a streaming/upsert implementation.
Source is the local copy of lake/v1/data/raw downloaded from the private bucket.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil


def day(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def utc(text: str) -> datetime:
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("Manifest timestamps must be UTC")
    return value


def digest(file: Path) -> str:
    h = hashlib.sha256()
    with file.open("rb") as reader:
        for block in iter(lambda: reader.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def overlaps(m: dict, start: datetime, end: datetime) -> bool:
    left, right = utc(m["start_utc"]), utc(m["end_utc"])
    if left >= right:
        raise ValueError("Invalid manifest time window")
    return left < end and right > start


def select(source: Path, dest: Path, aq_date: str, firms_date: str,
           era5_date: str, region: str) -> dict:
    source = source.resolve()
    dest = dest.resolve()
    if not source.is_dir() or source.name != "raw":
        raise ValueError("Source must be a directory named data/raw")
    if dest.exists():
        raise FileExistsError(f"Refusing existing job workspace: {dest}")
    aq = day(aq_date)
    fire = day(firms_date)
    weather = day(era5_date)
    for folder in ("src", "configs", "contracts"):
        if not (Path.cwd() / folder).is_dir():
            raise ValueError(f"Repository folder missing: {folder}")
    counters = Counter()
    selected = []
    root = source.parents[1]  # source is <root>/data/raw
    for mf in sorted(source.rglob("*.manifest.json")):
        if mf.is_symlink():
            raise ValueError("Manifest symlink not allowed")
        m = json.loads(mf.read_text(encoding="utf-8"))
        src, product = m.get("source"), m.get("source_product")
        if m.get("schema_version") != "manifest-v1" or m.get("status") not in ("success", "empty"):
            raise ValueError(f"Unsupported manifest: {mf}")
        key = None
        if src == "openaq" and product == "openaq_v3_locations":
            key = "aq_locations"  # Keep sensor metadata for all selected AQ hours
        elif src == "openaq" and product == "openaq_v3_hours":
            if overlaps(m, aq - timedelta(days=30), aq + timedelta(days=1)):
                key = "aq_hours"
        elif src == "nasa_firms" and product in ("VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT"):
            if overlaps(m, fire, fire + timedelta(days=1)):
                key = "firms"
        elif src == "era5" and product == "reanalysis-era5-single-levels" and m.get("region_id") == region:
            # Deliberately reject monthly GRIB: it can decode millions of rows for a one-day job.
            if overlaps(m, weather, weather + timedelta(days=1)):
                if utc(m["end_utc"]) - utc(m["start_utc"]) > timedelta(days=1):
                    raise ValueError("ERA5 monthly GRIB overlaps the chosen day; use a daily file or an explicit monthly-batch workflow")
                key = "era5"
        if key is None:
            continue
        relative = m.get("file")
        if not isinstance(relative, str) or "\\" in relative:
            raise ValueError("Unsafe Raw reference")
        rel = PurePosixPath(relative)
        if rel.is_absolute() or ".." in rel.parts or rel.parts[:3] != ("data", "raw", src):
            raise ValueError(f"Unsafe manifest Raw path: {relative}")
        candidate = root / Path(*rel.parts)
        raw = candidate.resolve()
        if not raw.is_relative_to(source) or candidate.is_symlink() or not raw.is_file():
            raise ValueError(f"Missing/unsafe Raw: {relative}")
        if raw.stat().st_size != m.get("bytes") or digest(raw) != m.get("sha256"):
            raise ValueError(f"Manifest SHA-256 mismatch: {relative}")
        if m.get("format") != {"openaq": "json", "nasa_firms": "csv", "era5": "grib"}[src]:
            raise ValueError(f"Unexpected Raw format: {relative}")
        selected.append((mf, raw))
        counters[key] += 1
    missing = [key for key in ("aq_locations", "aq_hours", "firms", "era5") if not counters[key]]
    if missing:
        raise ValueError(f"Cannot run six tables; missing selected inputs: {', '.join(missing)}")
    # Validate everything before touching the destination.
    dest.mkdir(parents=True)
    for mf, raw in selected:
        for f in (mf, raw):
            rel = f.relative_to(root)
            out = dest / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, out)
    for folder in ("src", "configs", "contracts"):
        origin = Path.cwd() / folder
        shutil.copytree(origin, dest / folder, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    result = {
        "aq_day": aq_date, "firms_day": firms_date, "era5_day": era5_date,
        "era5_region": region, "selected_manifests": dict(counters),
        "manifest_checksums": {mf.relative_to(root).as_posix(): digest(mf) for mf, _ in selected},
        "note": "AQ includes 30-day history; FIRMS files may span several days; ERA5 is restricted to daily files in one region. This is a bounded batch, not full incremental upsert.",
    }
    (dest / "SELECTION.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--dest", type=Path, required=True)
    p.add_argument("--aq-date", required=True)
    p.add_argument("--firms-date", required=True)
    p.add_argument("--era5-date", required=True)
    p.add_argument("--era5-region", choices=("HANOI", "VN_BBOX"), default="HANOI")
    args = p.parse_args()
    print("BATCH SELECT:", json.dumps(select(args.source, args.dest, args.aq_date, args.firms_date,
                                              args.era5_date, args.era5_region)["selected_manifests"], sort_keys=True))
    print("CHECKSUM PASS; bounded workspace ready; no provider calls, Spark or HF writes")


if __name__ == "__main__":
    main()
