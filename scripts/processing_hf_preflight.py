"""Verify HF Raw before Spark automation. Read-only."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import uuid

BUCKET = "CourseworkBigData/env-event-lake"
REMOTE = f"hf://buckets/{BUCKET}/lake/v1/data/raw"

SUPPORTED = {
    "openaq": ("openaq_v3_hours", "openaq_v3_locations"),
    "nasa_firms": ("VIIRS_NOAA20_NRT", "VIIRS_NOAA20_SP"),
    "era5": ("reanalysis-era5-single-levels",),
}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def audit(work):
    root = (work / "data/raw").resolve()
    manifests = sorted(root.rglob("*.manifest.json"))

    if not manifests:
        raise ValueError("No manifest-v1 files downloaded")

    counts = Counter()
    paths = set()

    for mf in manifests:
        if mf.is_symlink():
            raise ValueError("Unsafe manifest symlink")

        m = json.loads(mf.read_text(encoding="utf-8"))
        src = m.get("source")
        product = m.get("source_product")

        if (
            src not in SUPPORTED
            or product not in SUPPORTED[src]
            or m.get("schema_version") != "manifest-v1"
            or m.get("status") not in ("success", "empty")
            or m.get("acquisition_mode") not in ("live", "replay")
        ):
            raise ValueError(
                f"Unsupported/incomplete manifest: {mf.relative_to(work)}"
            )

        formats = {
            "openaq": "json",
            "nasa_firms": "csv",
            "era5": "grib",
        }

        if m.get("format") != formats[src]:
            raise ValueError(f"Wrong format: {mf.relative_to(work)}")

        rel = m.get("file")

        if not isinstance(rel, str) or "\\" in rel:
            raise ValueError("Invalid manifest file path")

        posix = PurePosixPath(rel)

        if (
            posix.is_absolute()
            or ".." in posix.parts
            or posix.parts[:3] != ("data", "raw", src)
        ):
            raise ValueError(f"Unsafe source path: {rel}")

        file = work.joinpath(*posix.parts)

        if (
            file.is_symlink()
            or not file.resolve().is_relative_to(root)
            or not file.is_file()
        ):
            raise ValueError(f"Missing/unsafe Raw: {rel}")

        if rel in paths:
            raise ValueError(f"Duplicate reference: {rel}")

        size = m.get("bytes")
        digest = m.get("sha256")
        rows = m.get("row_count")

        if (
            type(size) is not int
            or size < 0
            or type(rows) is not int
            or rows < 0
            or not isinstance(digest, str)
            or len(digest) != 64
        ):
            raise ValueError(f"Invalid manifest metadata: {rel}")

        if (
            file.stat().st_size != size
            or sha256(file).lower() != digest.lower()
        ):
            raise ValueError(f"SHA-256 / size mismatch: {rel}")

        if m["status"] == "empty" and rows:
            raise ValueError(f"Invalid empty status: {rel}")

        if src == "openaq":
            payload = json.loads(
                file.read_text(encoding="utf-8-sig")
            )

            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("results"), list)
            ):
                raise ValueError(f"Invalid OpenAQ results: {rel}")

            if len(payload["results"]) != rows:
                raise ValueError(f"OpenAQ count mismatch: {rel}")

        counts[(src, product)] += 1
        paths.add(rel)

    print("===== VERIFIED HF RAW =====")

    for (src, product), amount in sorted(counts.items()):
        print(f"{src}/{product}: {amount} manifest(s)")

    print(f"CHECKSUM PASS: {len(manifests)} manifests")

    missing = []

    for required in (
        ("openaq", "openaq_v3_hours"),
        ("openaq", "openaq_v3_locations"),
        ("era5", "reanalysis-era5-single-levels"),
    ):
        if not counts[required]:
            missing.append("/".join(required))

    if not sum(
        counts[("nasa_firms", p)]
        for p in SUPPORTED["nasa_firms"]
    ):
        missing.append("nasa_firms/VIIRS_NOAA20_{NRT,SP}")

    if missing:
        raise ValueError(
            "Not ready for all six tables: " + ", ".join(missing)
        )

    print(
        "HF RAW AUDIT: PASS; "
        "no Spark, provider calls, or cloud writes"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-raw-mib", type=int, default=256)
    args = parser.parse_args()

    if not 1 <= args.max_raw_mib <= 4096:
        parser.error("max-raw-mib must be 1..4096")

    work = (
        Path.cwd().resolve()
        / "data/processing_hf_audit"
        / ("run_" + uuid.uuid4().hex[:12])
    )

    local = work / "data/raw"
    local.parent.mkdir(parents=True)

    env = dict(os.environ)

    for secret in (
        "HF_WRITE_TOKEN",
        "OPENAQ_API_KEY",
        "FIRMS_MAP_KEY",
        "CDSAPI_KEY",
    ):
        env.pop(secret, None)

    if env.get("HF_READ_TOKEN"):
        env["HF_TOKEN"] = env.pop("HF_READ_TOKEN")

    print("Downloading HF Raw for read-only audit ...", flush=True)

    subprocess.run(
        ["hf", "buckets", "sync", REMOTE, str(local)],
        env=env,
        check=True,
    )

    total_bytes = sum(
        p.stat().st_size
        for p in local.rglob("*")
        if p.is_file()
    )

    if total_bytes > args.max_raw_mib * 1024 * 1024:
        raise ValueError(
            f"Snapshot too large: {total_bytes} bytes"
        )

    audit(work)


if __name__ == "__main__":
    try:
        main()
    except (
        ValueError,
        OSError,
        subprocess.CalledProcessError,
        json.JSONDecodeError,
    ) as error:
        print(
            f"HF RAW AUDIT: BLOCKED "
            f"({type(error).__name__}: {str(error)[:200]})",
            file=sys.stderr,
        )
        raise SystemExit(2)
