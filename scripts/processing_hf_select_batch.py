"""Build a bounded, checksum-verified Processing workspace from HF Raw manifests.

Manual acceptance batch only. A conflicting input is a hard failure, not a
reason to silently pick a provider revision. FIRMS SP > NRT is the explicit
project preference. No raw file is modified.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil


SOURCE_PRODUCTS = {
    "openaq": {"openaq_v3_hours", "openaq_v3_locations"},
    "nasa_firms": {"VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT"},
    "era5": {"reanalysis-era5-single-levels"},
}
FORMATS = {"openaq": "json", "nasa_firms": "csv", "era5": "grib"}


def day(text: str) -> datetime:
    d = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if d.strftime("%Y-%m-%d") != text:
        raise ValueError(f"Invalid UTC day: {text}")
    return d


def utc(text: str) -> datetime:
    if not isinstance(text, str):
        raise ValueError("Manifest timestamp must be a UTC string")
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


def interval(m: dict) -> tuple[datetime, datetime]:
    left, right = utc(m["start_utc"]), utc(m["end_utc"])
    if left >= right:
        raise ValueError("Invalid manifest time window")
    return left, right


def overlaps(m: dict, start: datetime, end: datetime) -> bool:
    left, right = interval(m)
    return left < end and right > start


def canonical_nonoverlapping(rows: list[dict], label: str) -> tuple[list[dict], list[dict]]:
    """Allow disjoint windows and byte-identical replays; reject disagreement.

    A conflicting manifest is intentionally NOT resolved by timestamp or
    filename: acquisition time does not establish data scientific authority.
    """
    chosen: list[dict] = []
    redundant: list[dict] = []
    for row in sorted(rows, key=lambda r: (interval(r["manifest"]), r["relative"])):
        win = interval(row["manifest"])
        same = next((old for old in chosen if win[0] < interval(old["manifest"])[1]
                     and interval(old["manifest"])[0] < win[1]), None)
        if same is None:
            chosen.append(row)
        elif win == interval(same["manifest"]) and row["manifest"]["sha256"] == same["manifest"]["sha256"]:
            redundant.append(row)
        else:
            raise ValueError(
                f"Conflicting {label} manifests for overlapping UTC interval: "
                f"{same['relative']} <> {row['relative']}. "
                "Require an explicit reviewed canonical decision; no silent overwrite."
            )
    return chosen, redundant


def select(source: Path, dest: Path, aq_date: str, firms_date: str,
           era5_date: str, region: str) -> dict:
    source, dest = source.resolve(), dest.resolve()
    if not source.is_dir() or source.name != "raw" or source.parent.name != "data":
        raise ValueError("Source must be a data/raw directory")
    if dest.exists():
        raise FileExistsError(f"Refusing existing job workspace: {dest}")
    if dest == source or dest.is_relative_to(source) or source.is_relative_to(dest):
        raise ValueError("Destination must not contain or overlap Raw source")
    if region not in ("HANOI", "VN_BBOX"):
        raise ValueError("Unsupported ERA5 region")
    aq, fire, weather = day(aq_date), day(firms_date), day(era5_date)
    for folder in ("src", "configs", "contracts"):
        if not (Path.cwd() / folder).is_dir():
            raise ValueError(f"Repository folder missing: {folder}")

    root = source.parents[1]
    candidates: dict[str, list[dict]] = {k: [] for k in ("aq_hours", "aq_locations", "firms_sp", "firms_nrt", "era5")}
    for mf in sorted(source.rglob("*.manifest.json")):
        if mf.is_symlink() or not mf.resolve().is_relative_to(source):
            raise ValueError(f"Unsafe manifest: {mf}")
        m = json.loads(mf.read_text(encoding="utf-8"))
        src, product = m.get("source"), m.get("source_product")
        if src not in SOURCE_PRODUCTS or product not in SOURCE_PRODUCTS[src]:
            raise ValueError(f"Unknown provider/product manifest: {mf}")
        if m.get("schema_version") != "manifest-v1" or m.get("status") not in ("success", "empty"):
            raise ValueError(f"Unsupported manifest: {mf}")
        if m.get("format") != FORMATS[src]:
            raise ValueError(f"Unexpected Raw format: {mf}")

        key = None
        if src == "openaq" and product == "openaq_v3_locations":
            key = "aq_locations"
        elif src == "openaq" and product == "openaq_v3_hours":
            if overlaps(m, aq - timedelta(days=30), aq + timedelta(days=1)):
                key = "aq_hours"
        elif src == "nasa_firms":
            if m.get("region_id") == "VN_BBOX" and overlaps(m, fire, fire + timedelta(days=1)):
                key = "firms_sp" if product == "VIIRS_NOAA20_SP" else "firms_nrt"
        elif src == "era5" and m.get("region_id") == region:
            if overlaps(m, weather, weather + timedelta(days=1)):
                if interval(m)[1] - interval(m)[0] > timedelta(days=1):
                    raise ValueError("Overlapping ERA5 monthly GRIB is not supported by daily batch")
                key = "era5"
        if key is None:
            continue
        relative = m.get("file")
        if not isinstance(relative, str) or "\\" in relative:
            raise ValueError(f"Unsafe Raw reference in {mf}")
        rel = PurePosixPath(relative)
        if (rel.is_absolute() or ".." in rel.parts or "." in rel.parts
                or rel.parts[:3] != ("data", "raw", src)):
            raise ValueError(f"Unsafe Raw path: {relative}")
        candidate = root / Path(*rel.parts)
        raw = candidate.resolve()
        if not raw.is_relative_to(source) or candidate.is_symlink() or not raw.is_file():
            raise ValueError(f"Missing/unsafe Raw: {relative}")
        if raw.stat().st_size != m.get("bytes") or digest(raw) != m.get("sha256"):
            raise ValueError(f"Manifest SHA-256 mismatch: {relative}")
        if (src == "era5" and (product not in raw.parts or region not in raw.parts)) or (
            src == "nasa_firms" and product not in raw.parts):
            raise ValueError(f"Manifest product/region path mismatch: {relative}")
        if not isinstance(m.get("row_count"), int) or m["row_count"] < 0:
            raise ValueError(f"Invalid row count: {mf}")
        candidates[key].append({"mf": mf, "raw": raw, "relative": relative, "manifest": m})

    era5, redundant_era5 = canonical_nonoverlapping(candidates["era5"], "ERA5")
    fir_source = "VIIRS_NOAA20_SP" if candidates["firms_sp"] else "VIIRS_NOAA20_NRT"
    fir_key = "firms_sp" if candidates["firms_sp"] else "firms_nrt"
    firms, redundant_firms = canonical_nonoverlapping(candidates[fir_key], f"FIRMS {fir_source}")
    selected_by_kind = {
        "aq_hours": candidates["aq_hours"],
        "aq_locations": candidates["aq_locations"],
        "era5": era5,
        "firms": firms,
    }
    missing = [key for key, records in selected_by_kind.items() if not records]
    if missing:
        raise ValueError("Cannot run all six tables: missing selected inputs: " + ", ".join(missing))

    chosen = [row for group in selected_by_kind.values() for row in group]
    duplicate_files = [relative for relative, count in Counter(row["relative"] for row in chosen).items() if count != 1]
    if duplicate_files:
        raise ValueError(f"Duplicate Raw file in selected manifests: {duplicate_files}")
    # Validate every dependency before writing the new workspace.
    dest.mkdir(parents=True)
    try:
        for row in chosen:
            for f in (row["mf"], row["raw"]):
                rel = f.relative_to(root)
                out = dest / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, out)
        for folder in ("src", "configs", "contracts"):
            shutil.copytree(Path.cwd() / folder, dest / folder,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        result = {
            "aq_day": aq_date, "firms_day": firms_date, "era5_day": era5_date,
            "era5_region": region, "firms_preferred_product": fir_source,
            "selected_manifests": {key: len(group) for key, group in selected_by_kind.items()},
            "manifest_checksums": {row["mf"].relative_to(root).as_posix(): digest(row["mf"]) for row in chosen},
            "excluded_identical_replays": [row["relative"] for row in redundant_era5 + redundant_firms],
            "note": "Manual bounded run; SP preferred over NRT. ERA5/FIRMS conflicting overlapping windows fail closed; daily published scope applied after processing.",
        }
        (dest / "SELECTION.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except BaseException:
        shutil.rmtree(dest)
        raise
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--dest", type=Path, required=True)
    p.add_argument("--aq-date", required=True)
    p.add_argument("--firms-date", required=True)
    p.add_argument("--era5-date", required=True)
    p.add_argument("--era5-region", choices=("HANOI", "VN_BBOX"), default="HANOI")
    a = p.parse_args()
    result = select(a.source, a.dest, a.aq_date, a.firms_date, a.era5_date, a.era5_region)
    print("BATCH SELECT:", json.dumps(result["selected_manifests"], sort_keys=True))
    print("FIRMS PREFERENCE:", result["firms_preferred_product"])
    print("CHECKSUM PASS; bounded workspace ready; no provider calls, Spark or HF writes")


if __name__ == "__main__":
    main()
