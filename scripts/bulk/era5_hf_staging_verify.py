#!/usr/bin/env python3
"""Independent, READ-ONLY SHA/provenance verification of monthly ERA5 HF staging.

This is NOT lake/v1 promotion. No CDS API, no HF writes, no local Raw writes.
Run with `plan` offline; `verify` requires HF_READ_TOKEN and explicit approval.
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Callable

BUCKET = "CourseworkBigData/env-event-lake"
PREFIX = "experiments/era5_monthly_vn_bbox_v1"
DATASET = "reanalysis-era5-single-levels"
BBOX_NWSE = [24, 102, 8, 110]
VARIABLES = ["2m_temperature", "2m_dewpoint_temperature",
             "10m_u_component_of_wind", "10m_v_component_of_wind",
             "total_precipitation"]
MONTHS = ("2023-08", "2023-09", "2023-10", "2023-11")
MAX_RAW_BYTES = 128 * 1024 * 1024
MAX_JSON_BYTES = 64 * 1024


def key_for(month: str, kind: str) -> str:
    if kind == "state":
        return f"{PREFIX}/state/{month}.json"
    if kind == "manifest":
        return f"{PREFIX}/verified/{month}/manifest.json"
    if kind == "raw":
        return f"{PREFIX}/verified/{month}/era5_vn_bbox_{month}.grib"
    raise ValueError("unsupported kind")


def expected_fields(month: str) -> int:
    y, m = (int(x) for x in month.split("-"))
    return calendar.monthrange(y, m)[1] * 24 * 5


def expected_request(month: str) -> dict:
    y, mo = month.split("-")
    dates = calendar.monthrange(int(y), int(mo))[1]
    return {"product_type": ["reanalysis"], "variable": list(VARIABLES),
            "year": [y], "month": [mo],
            "day": [f"{d:02d}" for d in range(1, dates + 1)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": [24.0, 102.0, 8.0, 110.0],
            "data_format": "grib", "download_format": "unarchived"}


def expected_request_sha(month: str) -> str:
    payload = json.dumps({"dataset": DATASET, "request": expected_request(month)},
                         sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def digest(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while part := f.read(1024 * 1024):
            size += len(part)
            h.update(part)
    return size, h.hexdigest()


def require(flag: bool, reason: str) -> None:
    if not flag:
        raise ValueError(reason)


def check_json(doc: dict, month: str, kind: str) -> None:
    require(isinstance(doc, dict), f"{month} {kind} is not a JSON object")
    fields = expected_fields(month)
    require(doc.get("month") == month, f"{month} {kind} month mismatch")
    require(doc.get("dataset") == DATASET, f"{month} {kind} dataset mismatch")
    require(doc.get("expected_fields") == fields, f"{month} {kind} field count mismatch")
    require(doc.get("audit_status") == "PASS", f"{month} {kind} audit not PASS")
    require(type(doc.get("size_bytes")) is int and 0 < doc["size_bytes"] <= MAX_RAW_BYTES,
            f"{month} {kind} size invalid")
    require(bool(re.fullmatch(r"[a-f0-9]{64}", str(doc.get("sha256", "")))),
            f"{month} {kind} SHA invalid")
    require(bool(re.fullmatch(r"[a-f0-9-]{36}", str(doc.get("request_id", "")), re.I)),
            f"{month} {kind} request ID invalid")
    require(doc.get("request_sha256") == expected_request_sha(month),
            f"{month} {kind} CDS request SHA does not match exact monthly scope")
    if kind == "state":
        require(doc.get("status") == "PASS", f"{month} remote queue state is not PASS")
        require(doc.get("request") == expected_request(month),
                f"{month} state request parameters mismatch")
        require(doc.get("audited_grib_messages") == fields and
                doc.get("unique_date_hour_parameter") == fields and
                doc.get("grid_dimensions") == "33x65",
                f"{month} recorded semantic audit results incomplete")
    else:
        require(doc.get("bbox_nwse") == BBOX_NWSE, f"{month} bounding box mismatch")
        require(doc.get("data_format") == "grib", f"{month} file format mismatch")
        require(doc.get("cloud_prefix") == PREFIX, f"{month} namespace mismatch")
        require(doc.get("variable") == VARIABLES,
                f"{month} five expected variables mismatch")
        require(doc.get("audit_method") == "ecCodes date/hour/parameter/grid + SHA-256",
                f"{month} semantic audit method mismatch")


def reconcile_month(month: str, state: dict, manifest: dict, raw_size: int, raw_sha: str) -> dict:
    check_json(state, month, "state")
    check_json(manifest, month, "manifest")
    for key in ("month", "dataset", "expected_fields", "audit_status", "request_id",
                "request_sha256", "size_bytes", "sha256"):
        require(state[key] == manifest[key], f"{month} state/manifest {key} conflict")
    require((raw_size, raw_sha) == (state["size_bytes"], state["sha256"]),
            f"{month} downloaded GRIB bytes/SHA mismatch")
    return {"month": month, "status": "VERIFIED_READ_ONLY", "raw_key": key_for(month, "raw"),
            "manifest_key": key_for(month, "manifest"), "state_key": key_for(month, "state"),
            "expected_fields": expected_fields(month), "bytes": raw_size,
            "sha256": raw_sha, "request_id": state["request_id"],
            "canonical_status": "NOT_PROMOTED_OR_VALIDATED"}


def parse_months(raw: str) -> tuple[str, ...]:
    months = tuple(x.strip() for x in raw.split(",") if x.strip())
    require(bool(months) and len(months) == len(set(months)), "duplicate/empty month selection")
    require(all(m in MONTHS for m in months), "months not in the reviewed 2023-08..11 pilot")
    return months


def verify_remote(months: tuple[str, ...], *, bucket: str, token: str,
                  get_info: Callable, list_tree: Callable, download: Callable) -> list[dict]:
    require(bool(token.strip()), "HF_READ_TOKEN missing")
    require(bucket == BUCKET, "unreviewed bucket path")
    info = get_info(bucket, token=token)
    require(getattr(info, "private", None) is True, "bucket is not private")
    names = {obj.path for obj in list_tree(bucket, prefix=PREFIX + "/", recursive=True,
                                           token=token) if getattr(obj, "type", "file") == "file"}
    required = {key_for(m, kind) for m in months for kind in ("raw", "manifest", "state")}
    require(required <= names, f"remote verified objects missing: {len(required - names)}")
    results = []
    with tempfile.TemporaryDirectory(prefix="era5-hf-staging-verify-") as t:
        base = Path(t)
        for month in months:
            paths = {kind: base / f"{month}-{kind}" for kind in ("raw", "manifest", "state")}
            pairs = [(key_for(month, kind), str(path)) for kind, path in paths.items()]
            download(bucket, files=pairs, token=token, raise_on_missing_files=True)
            for kind, path in paths.items():
                limit = MAX_RAW_BYTES if kind == "raw" else MAX_JSON_BYTES
                require(path.is_file() and 0 < path.stat().st_size <= limit,
                        f"{month} {kind} missing or exceeds safety limit")
            state = json.loads(paths["state"].read_text(encoding="utf-8"))
            manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
            raw_size, raw_sha = digest(paths["raw"])
            results.append(reconcile_month(month, state, manifest, raw_size, raw_sha))
            print(f"HF_STAGING_VERIFIED month={month} fields={expected_fields(month)} "
                  f"bytes={raw_size} sha256={raw_sha}", flush=True)
    return results


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("plan", "verify"))
    p.add_argument("--months", default=",".join(MONTHS))
    p.add_argument("--approve-hf-read", action="store_true")
    p.add_argument("--report", default="")
    args = p.parse_args()
    try:
        months = parse_months(args.months)
        if args.command == "plan":
            print(json.dumps({"mode": "OFFLINE_PLAN", "months": months,
                              "expected_fields": {m: expected_fields(m) for m in months},
                              "hf_reads": 0, "hf_writes": 0, "cds_calls": 0,
                              "canonical_writes": 0}, indent=2))
            return 0
        require(args.approve_hf_read, "read approval required")
        from huggingface_hub import bucket_info, list_bucket_tree, download_bucket_files
        results = verify_remote(months, bucket=BUCKET, token=os.environ.get("HF_READ_TOKEN", ""),
                                get_info=bucket_info, list_tree=list_bucket_tree,
                                download=download_bucket_files)
        report = {"result": "PASS", "scope": "STAGING_SHA_AND_PROVENANCE_ONLY",
                  "semantic_audit": "RELY_ON_UPSTREAM_QUEUE_MANIFEST; NOT_RE-RUN",
                  "remote_writes": 0, "cds_calls": 0, "canonical_writes": 0,
                  "months": results, "total_bytes": sum(x["bytes"] for x in results)}
        if args.report:
            path = Path(args.report)
            require(not path.exists(), "report path already exists; refuse overwrite")
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
                f.write("\n")
            print(f"LOCAL_REPORT_CREATED {path}")
        print(f"HF_STAGING_READ_ONLY_PASS months={len(results)} bytes={report['total_bytes']}")
        print("NO HF/CDS/CANONICAL WRITES; monthly manifests NOT manifest-v1")
        return 0
    except Exception as exc:
        # The provider may attach sensitive information to exceptions: do not echo it.
        print(f"STOP: {type(exc).__name__}; no further reads or any cloud writes")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
