#!/usr/bin/env python3
"""Guarded ERA5 VN_BBOX bulk ingestion to isolated LOCAL STAGING ONLY.

NO modification of lake/v1, Raw/Manifest, data contracts, or Hugging Face.
Network only with explicit submit/check/adopt/fetch. Audit requires ecCodes.

Commands:
  plan --from-month YYYY-MM --to-month YYYY-MM (offline, read-only)
  inspect --month YYYY-MM                       (offline, read-only)
  submit --month YYYY-MM                        (one CDS job; never auto-repeat)
  adopt --month YYYY-MM --request-id UUID       (rebind existing CDS job)
  check --month YYYY-MM                         (inspect known remote job)
  fetch --month YYYY-MM                         (download known job to staging)
  audit --month YYYY-MM                         (local content validation)
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import sys
import tempfile
import uuid
from collections import Counter
from contextlib import contextmanager

DATASET = "reanalysis-era5-single-levels"
VAR_NAMES = (
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "total_precipitation",
)
# ERA5 paramIds for 2t, 2d, 10u, 10v, tp
PARAM_IDS = frozenset((167, 168, 165, 166, 228))
AREA = [24.0, 102.0, 8.0, 110.0]  # N W S E
HOURS = [f"{i:02d}:00" for i in range(24)]
# Snapshot of independently audited VN_BBOX lake/v1 days supplied by operator.
# This table is deliberately not generalized to other regions.
PREVIOUSLY_COMPLETE = frozenset(
    f"2024-11-{day:02d}" for day in (13, 14, 15, 16)
)
DEFAULT_MAX_FIELDS = 60000  # conservative historical CDS limit, not a quota guarantee
DEFAULT_MAX_BYTES = 128 * 1024 * 1024  # 128 MiB per month transfer guard
CDS_URL = "https://cds.climate.copernicus.eu/api"


def month_to_dates(month: str) -> list[str]:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError("Month must be YYYY-MM")
    y, m = map(int, month.split("-"))
    if not (1940 <= y <= 2025):
        raise ValueError("Guarded historical scope: 1940..2025 only (no recent ERA5T)")
    return [f"{month}-{d:02d}" for d in range(1, calendar.monthrange(y, m)[1] + 1)]


def plan_month(month: str) -> dict:
    all_dates = month_to_dates(month)
    selected = [d for d in all_dates if d not in PREVIOUSLY_COMPLETE]
    skipped = [d for d in all_dates if d in PREVIOUSLY_COMPLETE]
    year, number = month.split("-")
    request = {
        "product_type": ["reanalysis"],
        "variable": list(VAR_NAMES),
        "year": [year],
        "month": [number],
        "day": [d[-2:] for d in selected],
        "time": HOURS,
        "area": AREA,
        "data_format": "grib",
        "download_format": "unarchived",
    }
    fields = 5 * 24 * len(selected)
    canonical = json.dumps({"dataset": DATASET, "request": request}, sort_keys=True,
                           separators=(",", ":"))
    return {
        "month": month,
        "dataset": DATASET,
        "request": request,
        "request_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "selected_dates": selected,
        "excluded_pass_dates": skipped,
        "expected_fields": fields,
        "estimate_mib_at_527760_bytes_per_day": round(len(selected) * 527760 / 1048576, 2),
    }


def months_between(a: str, b: str) -> list[str]:
    month_to_dates(a)
    month_to_dates(b)
    y1, m1 = map(int, a.split("-"))
    y2, m2 = map(int, b.split("-"))
    if (y1, m1) > (y2, m2):
        raise ValueError("from-month is after to-month")
    if (y2-y1) * 12 + m2 - m1 > 119:
        raise ValueError("Planning at most 120 months per invocation")
    out = []
    while (y1, m1) <= (y2, m2):
        out.append(f"{y1:04d}-{m1:02d}")
        m1 += 1
        if m1 == 13:
            y1 += 1
            m1 = 1
    return out


def staging_root() -> pathlib.Path:
    cwd = pathlib.Path.cwd().resolve()
    # Fixed, isolated root; no option to direct the program into raw/cloud locations.
    return cwd / "data" / "bulk_staging" / "era5_monthly_vn_bbox"


def paths_for(month: str) -> dict[str, pathlib.Path]:
    base = staging_root() / month
    return {
        "dir": base,
        "state": base / "job.json",
        "grib": base / f"era5_vn_bbox_{month}.grib",
        "lock": base / ".operation.lock",
    }


def write_json_atomic(path: pathlib.Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tempname = tempfile.mkstemp(prefix=".atomic-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tempname, path)  # replaces ONLY our own state metadata
    finally:
        if os.path.exists(tempname):
            os.unlink(tempname)


@contextmanager
def locked(month: str):
    p = paths_for(month)
    p["dir"].mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(p["lock"]), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise RuntimeError("Lock exists. Check running process; do not remove blindly.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()} utc={now()}\n")
        yield p
    finally:
        try:
            p["lock"].unlink()
        except FileNotFoundError:
            pass


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def load_state(month: str, must_exist: bool = True) -> dict | None:
    p = paths_for(month)
    if not p["state"].is_file():
        if must_exist:
            raise RuntimeError("No local job.json. Use submit ONCE or adopt existing request ID.")
        return None
    state = json.loads(p["state"].read_text(encoding="utf-8"))
    expected = plan_month(month)
    if state.get("request_sha256") != expected["request_sha256"]:
        raise RuntimeError("Request hash mismatch: STOP, do not overwrite local state")
    return state


def save_state(month: str, state: dict) -> None:
    state["updated_at_utc"] = now()
    write_json_atomic(paths_for(month)["state"], state)


def safe_summary(state: dict) -> dict:
    keys = ["month", "status", "request_id", "expected_fields", "remote_status",
            "size_bytes", "sha256", "audit_status", "created_at_utc", "updated_at_utc"]
    return {key: state.get(key) for key in keys if key in state}


def credentials_client():
    # Private credentials never printed. Reuse the existing .cdsapirc from CDS.
    try:
        from ecmwf.datastores import Client
    except ImportError as e:
        raise RuntimeError("Install first: python -m pip install ecmwf-datastores-client") from e
    rc = pathlib.Path.home() / ".cdsapirc"
    if not rc.is_file():
        raise RuntimeError(f"Missing credentials file: {rc} (contents must remain private)")
    creds = {}
    for line in rc.read_text(encoding="utf-8-sig").splitlines():
        if line.strip().startswith("#") or ":" not in line:
            continue
        k, v = line.split(":", 1)
        if k.strip() in ("key", "url"):
            creds[k.strip()] = v.strip().strip("'\"")
    key = creds.get("key")
    url = creds.get("url", CDS_URL).rstrip("/")
    if not key or url != CDS_URL:
        raise RuntimeError("CDS credentials missing or API URL not the expected CDS endpoint")
    return Client(url=url, key=key, cleanup=False, progress=True,
                  maximum_tries=5, sleep_max=45)


def normalize_value(key: str, val):
    if key == "area":
        if not isinstance(val, (list, tuple)) or len(val) != 4:
            return None
        return tuple(round(float(x), 5) for x in val)
    if isinstance(val, (list, tuple)):
        return tuple(sorted(str(x) for x in val))
    return (str(val),)


def assert_same_remote_request(remote, planned: dict) -> None:
    if remote.collection_id != DATASET:
        raise RuntimeError("Remote collection != expected dataset")
    actual = remote.request
    if not isinstance(actual, dict):
        raise RuntimeError("Remote API did not provide verifiable request metadata")
    for k, expected in planned["request"].items():
        if k not in actual:
            raise RuntimeError(f"Remote request lacks {k}; cannot verify provenance")
        if normalize_value(k, actual[k]) != normalize_value(k, expected):
            raise RuntimeError(f"Remote request mismatch in {k}; do NOT adopt/download")


def do_plan(args):
    months = months_between(args.from_month, args.to_month)
    plans = [plan_month(m) for m in months]
    if any(p["expected_fields"] > args.max_fields for p in plans):
        raise RuntimeError("A month exceeds the conservative --max-fields gate")
    result = {
        "mode": "OFFLINE READ-ONLY PLAN (CDS requests=0; downloads=0; writes=0)",
        "staging_root": str(staging_root()),
        "months": len(plans),
        "selected_days": sum(len(p["selected_dates"]) for p in plans),
        "excluded_pass_days": sorted(set(d for p in plans for d in p["excluded_pass_dates"])),
        "expected_fields": sum(p["expected_fields"] for p in plans),
        "extrapolated_MiB_GRIB_NOT_MEASURED": round(sum(len(p["selected_dates"]) * 527760 for p in plans)/1048576, 2),
        "jobs": [{"month": p["month"], "days": len(p["selected_dates"]),
                   "fields": p["expected_fields"], "request_sha256": p["request_sha256"]}
                  for p in plans],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


def do_inspect(args):
    plan = plan_month(args.month)
    state = load_state(args.month, must_exist=False)
    print(json.dumps({"mode": "OFFLINE READ-ONLY", "plan": plan,
                      "existing_state": safe_summary(state) if state else None,
                      "final_grib_exists": paths_for(args.month)["grib"].exists()},
                     ensure_ascii=False, indent=2))


def require_field_guard(planned, max_fields):
    if planned["expected_fields"] == 0:
        raise RuntimeError("No uncompleted day in month")
    if planned["expected_fields"] > max_fields:
        raise RuntimeError("CDS field-count conservative safety limit exceeded")


def do_submit(args):
    plan = plan_month(args.month)
    require_field_guard(plan, args.max_fields)
    with locked(args.month) as p:
        if p["grib"].exists() or p["state"].exists():
            raise RuntimeError("Job/data exists: never submit twice. Use check/fetch/audit/adopt.")
        client = credentials_client()  # local credential preflight, no CDS submission
        # BEFORE network call: durable intent prevents duplicate submission if interrupted.
        state = {**plan, "created_at_utc": now(), "status": "SUBMITTING",
                 "notes": "If interrupted before request_id stored, find it in CDS Your Requests and ADOPT. Never resubmit."}
        save_state(args.month, state)
        print("ONE CDS request begins. Interrupt before ID recorded => ADOPT, never retry submit.", flush=True)
        remote = client.submit(DATASET, plan["request"])
        rid = remote.request_id
        # Persist RID before any optional remote validation, to avoid losing recoverability.
        state["request_id"] = rid
        state["status"] = "SUBMITTED_NEEDS_REMOTE_VERIFICATION"
        save_state(args.month, state)
        assert_same_remote_request(remote, plan)
        state["status"] = "SUBMITTED"
        state["remote_status"] = remote.status
        save_state(args.month, state)
        print(json.dumps(safe_summary(state), indent=2))


def do_adopt(args):
    plan = plan_month(args.month)
    require_field_guard(plan, args.max_fields)
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,100}", args.request_id):
        raise ValueError("Invalid request ID format")
    with locked(args.month) as p:
        state = load_state(args.month, must_exist=False)
        if state and state.get("request_id") and state["request_id"] != args.request_id:
            raise RuntimeError("Different request_id already recorded. Stop and investigate")
        if p["grib"].exists():
            raise RuntimeError("Final GRIB already exists: no adopt on a populated target")
        client = credentials_client()
        remote = client.get_remote(args.request_id)
        assert_same_remote_request(remote, plan)
        if state is None:
            state = {**plan, "created_at_utc": now()}
        state.update(status="ADOPTED", request_id=args.request_id, remote_status=remote.status)
        save_state(args.month, state)
        print(json.dumps(safe_summary(state), indent=2))


def do_check(args):
    plan = plan_month(args.month)
    with locked(args.month):
        state = load_state(args.month)
        if not state.get("request_id"):
            raise RuntimeError("SUBMITTING but no ID. Check CDS Your Requests and use adopt.")
        remote = credentials_client().get_remote(state["request_id"])
        assert_same_remote_request(remote, plan)
        state["remote_status"] = remote.status
        state["remote_checked_at_utc"] = now()
        save_state(args.month, state)
        print(json.dumps(safe_summary(state), indent=2))
        print("results_ready =", remote.results_ready)


def sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def quick_grib_structure(path: pathlib.Path) -> int:
    """Reject ZIP/HTML/truncation. Not a substitute for eccodes semantic audit."""
    count = 0
    size = path.stat().st_size
    with path.open("rb") as f:
        while f.tell() < size:
            start = f.tell()
            hdr = f.read(16)
            if len(hdr) < 16 or hdr[:4] != b"GRIB":
                raise RuntimeError(f"Invalid GRIB header at byte {start} (ZIP/HTML/truncated?)")
            edition = hdr[7]
            if edition == 1:
                length = int.from_bytes(hdr[4:7], "big")
            elif edition == 2:
                length = int.from_bytes(hdr[8:16], "big")
            else:
                raise RuntimeError(f"Unexpected GRIB edition {edition} at {start}")
            if length < 20 or start + length > size:
                raise RuntimeError(f"Broken message length at {start}")
            f.seek(start + length - 4)
            if f.read(4) != b"7777":
                raise RuntimeError(f"Missing GRIB trailer at {start}")
            f.seek(start + length)
            count += 1
    return count


def do_fetch(args):
    plan = plan_month(args.month)
    with locked(args.month) as p:
        state = load_state(args.month)
        if not state.get("request_id"):
            raise RuntimeError("No request ID; use adopt, never resubmit")
        if p["grib"].exists():
            raise RuntimeError("Final GRIB already exists. Run audit; never overwrite")
        client = credentials_client()
        remote = client.get_remote(state["request_id"])
        assert_same_remote_request(remote, plan)
        state["remote_status"] = remote.status
        save_state(args.month, state)
        if not remote.results_ready:
            print("REMOTE NOT READY. Job preserved. Run check/fetch later; no new submission.")
            return
        results = client.get_results(state["request_id"])
        expected_bytes = results.content_length
        if expected_bytes is None:
            raise RuntimeError("Remote did not provide content_length; cannot enforce transfer budget")
        if expected_bytes <= 0 or expected_bytes > args.max_bytes:
            raise RuntimeError(f"Remote download bytes {expected_bytes} outside 1..{args.max_bytes}")
        incoming = p["dir"] / f".incoming_{uuid.uuid4().hex}.grib"
        state["status"] = "DOWNLOADING"
        save_state(args.month, state)
        try:
            # Official SDK download: job resumes via saved ID; file transfer may restart
            # from byte zero on interruption. Never claim HTTP Range resume.
            results.download(str(incoming))
            if not incoming.is_file() or not incoming.stat().st_size:
                raise RuntimeError("SDK download missing or empty")
            size = incoming.stat().st_size
            if size > args.max_bytes:
                raise RuntimeError("Transfer exceeded local max bytes; preserved for inspection")
            if expected_bytes is not None and size != expected_bytes:
                raise RuntimeError(f"Downloaded {size}, expected {expected_bytes}")
            n = quick_grib_structure(incoming)
            if n != plan["expected_fields"]:
                raise RuntimeError(f"Expected {plan['expected_fields']} GRIB messages, got {n}")
            checksum = sha256_file(incoming)
            # Link-based commit refuses to replace any existing final target, even races.
            os.link(incoming, p["grib"])
            incoming.unlink()
            state.update(status="DOWNLOADED_STRUCTURAL_OK", size_bytes=size,
                         sha256=checksum, counted_grib_messages=n)
            save_state(args.month, state)
            print(json.dumps(safe_summary(state), indent=2))
            print("BASIC STRUCTURE PASS; full variable/date/hour audit is still required.")
        except Exception:
            print("DOWNLOAD FAILED. Request ID preserved. Incomplete temporary file may remain:", incoming,
                  file=sys.stderr)
            raise


def do_audit(args):
    plan = plan_month(args.month)
    p = paths_for(args.month)
    # No writes on failure before the structural checks.
    state = load_state(args.month)
    if not p["grib"].is_file():
        raise RuntimeError("No complete staged GRIB file")
    sha = sha256_file(p["grib"])
    if sha != state.get("sha256") or p["grib"].stat().st_size != state.get("size_bytes"):
        raise RuntimeError("Stored SHA/size mismatch: file changed after download")
    count = quick_grib_structure(p["grib"])
    if count != plan["expected_fields"]:
        raise RuntimeError("GRIB count mismatch")
    try:
        from eccodes import codes_get, codes_grib_new_from_file, codes_release
    except ImportError as e:
        raise RuntimeError("Semantic audit REQUIRES ecCodes: python -m pip install eccodes") from e
    expected = {
        (int(d.replace("-", "")), t, param)
        for d in plan["selected_dates"]
        for t in range(24)
        for param in PARAM_IDS
    }
    seen = Counter()
    dimensions = Counter()
    with p["grib"].open("rb") as f:
        while True:
            msg = codes_grib_new_from_file(f)
            if msg is None:
                break
            try:
                date = int(codes_get(msg, "validityDate"))
                hhmm = int(codes_get(msg, "validityTime"))
                if hhmm % 100 != 0:
                    raise RuntimeError(f"Non-hourly timestamp HHMM={hhmm}")
                hour = hhmm // 100
                param = int(codes_get(msg, "paramId"))
                ni = int(codes_get(msg, "Ni"))
                nj = int(codes_get(msg, "Nj"))
                lat1 = float(codes_get(msg, "latitudeOfFirstGridPointInDegrees"))
                lon1 = float(codes_get(msg, "longitudeOfFirstGridPointInDegrees"))
                lat2 = float(codes_get(msg, "latitudeOfLastGridPointInDegrees"))
                lon2 = float(codes_get(msg, "longitudeOfLastGridPointInDegrees"))
                if not (abs(min(lat1, lat2) - 8) < 0.001
                        and abs(max(lat1, lat2) - 24) < 0.001
                        and abs(min(lon1, lon2) - 102) < 0.001
                        and abs(max(lon1, lon2) - 110) < 0.001):
                    raise RuntimeError(f"Grid bbox mismatch: {(lat1, lon1, lat2, lon2)}")
                dimensions[(ni, nj)] += 1
                if (ni, nj) != (33, 65):
                    raise RuntimeError(f"Unexpected grid dimensions {(ni, nj)}, expected 33x65")
                seen[(date, hour, param)] += 1
            finally:
                codes_release(msg)
    missing = expected - set(seen)
    extra = set(seen) - expected
    duplicates = [k for k, v in seen.items() if v != 1]
    if missing or extra or duplicates or sum(seen.values()) != count:
        samples = lambda xs: list(sorted(xs))[:8]
        raise RuntimeError("SEMANTIC AUDIT FAILED: " + json.dumps({
            "missing_count": len(missing), "missing_examples": samples(missing),
            "extra_count": len(extra), "extra_examples": samples(extra),
            "duplicate_count": len(duplicates), "duplicate_examples": samples(duplicates)
        }))
    with locked(args.month):
        state = load_state(args.month)
        if sha256_file(p["grib"]) != sha:
            raise RuntimeError("File changed during audit")
        state.update(status="AUDIT_PASS_LOCAL_STAGING_ONLY", audit_status="PASS",
                     audit_time_utc=now(), audited_grib_messages=count,
                     grid_dimensions="33x65", unique_date_hour_parameter=count)
        save_state(args.month, state)
    print("LOCAL STAGING AUDIT PASS (NO RAW/MANIFEST PUBLISH; NO HF UPLOAD)")
    print(json.dumps(safe_summary(state), indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="action", required=True)
    sp = sub.add_parser("plan", help="offline / zero writes, zero network")
    sp.add_argument("--from-month", required=True)
    sp.add_argument("--to-month", required=True)
    sp.add_argument("--max-fields", type=int, default=DEFAULT_MAX_FIELDS)
    for command in ("inspect", "submit", "adopt", "check", "fetch", "audit"):
        s = sub.add_parser(command)
        s.add_argument("--month", required=True)
        if command in ("submit", "adopt"):
            s.add_argument("--max-fields", type=int, default=DEFAULT_MAX_FIELDS)
        if command == "adopt":
            s.add_argument("--request-id", required=True)
        if command == "fetch":
            s.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    args = parser.parse_args()
    try:
        if args.action == "plan":
            do_plan(args)
        else:
            month_to_dates(args.month)
            getattr(sys.modules[__name__], "do_" + args.action)(args)
    except (ValueError, RuntimeError, OSError, KeyError) as e:
        print(f"STOP ({type(e).__name__}): {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
