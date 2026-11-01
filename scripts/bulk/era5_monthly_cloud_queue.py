#!/usr/bin/env python3
"""One-month-at-a-time ERA5 backfill into an existing PRIVATE HF Storage Bucket.

Fail closed: state/intent is uploaded *before* CDS submit. A missing request ID
is NEVER retried automatically. Completed files are SHA-verified in the cloud.
This is a separate experimental data namespace, NOT lake/v1 Raw/Manifest.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import tempfile
from dataclasses import dataclass
from typing import Any

try:
    from . import era5_monthly_bulk_staging as core  # imported as package in tests
except ImportError:
    import era5_monthly_bulk_staging as core  # executed directly by GitHub Actions

BUCKET = "CourseworkBigData/env-event-lake"
PREFIX = "experiments/era5_monthly_vn_bbox_v1"
# These months were audited in the user's local monthly backfill. Never issue
# new CDS jobs here for them. Extend only after independent audit verification.
ALREADY_AUDITED_MONTHS = frozenset(core.months_between("2024-02", "2025-12"))


def sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def state_path(month: str) -> str:
    return f"{PREFIX}/state/{month}.json"


def raw_path(month: str) -> str:
    return f"{PREFIX}/verified/{month}/era5_vn_bbox_{month}.grib"


def manifest_path(month: str) -> str:
    return f"{PREFIX}/verified/{month}/manifest.json"


def make_plan(from_month: str, to_month: str) -> list[str]:
    # Requested range is ascending; process most recent month first.
    all_months = core.months_between(from_month, to_month)
    # Deliberate hard bound: no overlap with the audited local ERA5 backlog.
    if any(m in ALREADY_AUDITED_MONTHS for m in all_months):
        raise RuntimeError("SCOPE OVERLAPS independently audited monthly data; STOP")
    if to_month > "2024-01":
        raise RuntimeError("Backfill cannot include months >= 2024-02")
    return list(reversed(all_months))


def scrub_state(state: dict[str, Any], month: str) -> dict[str, Any]:
    plan = core.plan_month(month)
    if state.get("month") != month or state.get("request_sha256") != plan["request_sha256"]:
        raise RuntimeError("Cloud job state/plan mismatch: STOP")
    if state.get("dataset") != core.DATASET or state.get("expected_fields") != plan["expected_fields"]:
        raise RuntimeError("Cloud job provenance mismatch: STOP")
    if state.get("status") == "PASS" and state.get("audit_status") != "PASS":
        raise RuntimeError("Unverified PASS marker: STOP")
    return state


@dataclass
class PrivateBucket:
    bucket: str
    read_token: str
    write_token: str

    def __post_init__(self) -> None:
        from huggingface_hub import bucket_info
        info = bucket_info(self.bucket, token=self.read_token)
        if getattr(info, "private", None) is not True:
            raise RuntimeError("Storage bucket is not PRIVATE: STOP before any upload")

    def names(self, prefix: str) -> set[str]:
        from huggingface_hub import list_bucket_tree
        return {
            entry.path for entry in list_bucket_tree(self.bucket, prefix=prefix,
                                                      recursive=True, token=self.read_token)
            if getattr(entry, "type", "file") == "file"
        }

    def download(self, remote: str, output: pathlib.Path) -> None:
        from huggingface_hub import download_bucket_files
        output.parent.mkdir(parents=True, exist_ok=True)
        download_bucket_files(self.bucket, files=[(remote, str(output))],
                              raise_on_missing_files=True, token=self.read_token)
        if not output.is_file():
            raise RuntimeError("Expected cloud object was not downloaded")

    def write_verified(self, local: pathlib.Path, remote: str) -> None:
        from huggingface_hub import batch_bucket_files
        if not local.is_file():
            raise RuntimeError("Missing file for cloud write")
        batch_bucket_files(self.bucket, add=[(str(local), remote)], token=self.write_token)
        with tempfile.TemporaryDirectory(prefix="era5-cloud-verify-") as t:
            tmp = pathlib.Path(t) / "download"
            self.download(remote, tmp)
            if (tmp.stat().st_size != local.stat().st_size or
                    core.sha256_file(tmp) != core.sha256_file(local)):
                raise RuntimeError("Cloud round-trip SHA/bytes mismatch: STOP")

    def write_state(self, month: str, state: dict[str, Any]) -> None:
        scrub_state(state, month)
        with tempfile.TemporaryDirectory(prefix="era5-cloud-state-") as t:
            local = pathlib.Path(t) / "state.json"
            local.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            self.write_verified(local, state_path(month))

    def state(self, month: str, known_names: set[str]) -> dict[str, Any] | None:
        if state_path(month) not in known_names:
            if raw_path(month) in known_names or manifest_path(month) in known_names:
                raise RuntimeError("Orphaned verified cloud object without queue state; STOP")
            return None
        with tempfile.TemporaryDirectory(prefix="era5-cloud-read-") as t:
            p = pathlib.Path(t) / "state.json"
            self.download(state_path(month), p)
            return scrub_state(json.loads(p.read_text(encoding="utf-8")), month)


def put_cds_rc() -> None:
    key = os.environ.get("CDSAPI_KEY", "").strip()
    if not key or "\n" in key or "\r" in key:
        raise RuntimeError("CDSAPI_KEY missing/invalid; no API calls")
    home = pathlib.Path.home() / ".cdsapirc"
    home.write_text(f"url: {core.CDS_URL}\nkey: {key}\n", encoding="utf-8")
    home.chmod(0o600)


def restore_local_state(month: str, state: dict[str, Any]) -> None:
    core.write_json_atomic(core.paths_for(month)["state"], state)


def publish_month(cloud: PrivateBucket, month: str, state: dict[str, Any],
                  names: set[str]) -> None:
    p = core.paths_for(month)["grib"]
    if state.get("audit_status") != "PASS" or not p.is_file():
        raise RuntimeError("Cannot publish unverified GRIB")
    size, sha = p.stat().st_size, core.sha256_file(p)
    if size != state.get("size_bytes") or sha != state.get("sha256"):
        raise RuntimeError("GRIB/record SHA mismatch before upload")
    remote_grib = raw_path(month)
    if remote_grib in names:
        # Immutable Raw policy in this namespace: never overwrite a differing file.
        with tempfile.TemporaryDirectory(prefix="era5-raw-cmp-") as t:
            tmp = pathlib.Path(t) / "raw.grib"
            cloud.download(remote_grib, tmp)
            if tmp.stat().st_size != size or core.sha256_file(tmp) != sha:
                raise RuntimeError("Conflicting verified GRIB in cloud; STOP")
    else:
        cloud.write_verified(p, remote_grib)
    provenance = {
        "month": month, "dataset": core.DATASET, "bbox_nwse": core.AREA,
        "variable": list(core.VAR_NAMES), "data_format": "grib",
        "request_id": state["request_id"],
        "request_sha256": state["request_sha256"],
        "expected_fields": state["expected_fields"], "audit_status": "PASS",
        "audit_method": "ecCodes date/hour/parameter/grid + SHA-256",
        "size_bytes": size, "sha256": sha, "audited_at_utc": state.get("audit_time_utc"),
        "cloud_prefix": PREFIX,
        "note": "VN BBOX, not Vietnam-country clipped. Separate from lake/v1.",
    }
    with tempfile.TemporaryDirectory(prefix="era5-manifest-") as t:
        mf = pathlib.Path(t) / "manifest.json"
        mf.write_text(json.dumps(provenance, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        if manifest_path(month) in names:
            # No blind overwrite of provenance.
            old = pathlib.Path(t) / "existing.json"
            cloud.download(manifest_path(month), old)
            prev = json.loads(old.read_text(encoding="utf-8"))
            if prev.get("sha256") != sha or prev.get("size_bytes") != size or prev.get("request_id") != state["request_id"]:
                raise RuntimeError("Different published manifest exists; STOP")
        else:
            cloud.write_verified(mf, manifest_path(month))
    state["status"] = "PASS"
    state["cloud_published_at_utc"] = utcnow()
    state["updated_at_utc"] = utcnow()
    cloud.write_state(month, state)
    print(f"PASS month={month} fields={state['expected_fields']} bytes={size} sha256={sha}")


def progress_one(cloud: PrivateBucket, month: str, state: dict[str, Any],
                 names: set[str]) -> None:
    rid = state.get("request_id")
    if not rid:
        raise RuntimeError(f"MONTH {month} has ambiguous INTENT without request_id. Check CDS and ADOPT; no retry")
    put_cds_rc()
    remote = core.credentials_client().get_remote(rid)
    core.assert_same_remote_request(remote, core.plan_month(month))
    state["remote_status"] = remote.status
    state["updated_at_utc"] = utcnow()
    if not remote.results_ready:
        # A rejected remote request is not silently re-submitted.
        if str(remote.status).lower() in ("failed", "rejected", "deleted"):
            state["status"] = "HOLD_REJECTED"
        cloud.write_state(month, state)
        print(f"WAIT month={month} remote_status={remote.status}; no new submission")
        return
    restore_local_state(month, state)
    try:
        if not core.paths_for(month)["grib"].is_file():
            core.do_fetch(argparse.Namespace(month=month, max_bytes=core.DEFAULT_MAX_BYTES))
        core.do_audit(argparse.Namespace(month=month))
    except Exception:
        # Remote job ID and its last known state remain durable in HF.
        raise
    done = core.load_state(month)
    publish_month(cloud, month, done, names)


def submit_one(cloud: PrivateBucket, month: str) -> None:
    plan = core.plan_month(month)
    core.require_field_guard(plan, core.DEFAULT_MAX_FIELDS)
    put_cds_rc()
    client = core.credentials_client()
    # Write immutable recovery barrier BEFORE network submission.
    state = {**plan, "created_at_utc": utcnow(), "updated_at_utc": utcnow(),
             "status": "SUBMISSION_INTENT_NO_ID",
             "recovery": "If ID is absent, check CDS Your Requests and adopt manually. NEVER auto-resubmit."}
    cloud.write_state(month, state)
    print(f"SAVED PRE-SUBMIT INTENT month={month}; CDS submission begins", flush=True)
    remote = client.submit(core.DATASET, plan["request"])
    rid = remote.request_id
    state.update(request_id=rid, status="SUBMITTED", remote_status=remote.status,
                 updated_at_utc=utcnow())
    cloud.write_state(month, state)
    # Remote post-submit validation; ID was already persisted.
    core.assert_same_remote_request(remote, plan)
    print(f"SUBMITTED month={month} request_id={rid} fields={plan['expected_fields']}")


def adopt_one(cloud: PrivateBucket, month: str, rid: str) -> None:
    if not re.fullmatch(r"[a-f0-9-]{32,40}", rid, re.I):
        raise RuntimeError("Invalid remote CDS request ID")
    names = cloud.names(PREFIX + "/")
    state = cloud.state(month, names)
    if state is None:
        raise RuntimeError("No durable intent for this month; refuse unexpected adoption")
    if state.get("request_id") and state["request_id"] != rid:
        raise RuntimeError("Other request ID already bound; refuse replacement")
    put_cds_rc()
    remote = core.credentials_client().get_remote(rid)
    core.assert_same_remote_request(remote, core.plan_month(month))
    state.update(request_id=rid, remote_status=remote.status, status="ADOPTED", updated_at_utc=utcnow())
    cloud.write_state(month, state)
    print(f"ADOPTED month={month} request_id={rid}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["plan", "tick", "adopt"])
    p.add_argument("--from-month", required=True)
    p.add_argument("--to-month", required=True)
    p.add_argument("--bucket", default=BUCKET)
    p.add_argument("--approve-api", action="store_true")
    p.add_argument("--approve-cloud", action="store_true")
    p.add_argument("--allow-new-submission", action="store_true")
    p.add_argument("--reviewed-cds-request-history", action="store_true")
    p.add_argument("--month")
    p.add_argument("--request-id")
    args = p.parse_args()
    months = make_plan(args.from_month, args.to_month)
    if args.command == "plan":
        print(json.dumps({"mode": "OFFLINE PLAN, ZERO API/CLOUD CALLS", "months": months,
                          "monthly_requests_max": len(months),
                          "per_invocation_new_submissions_max": 1,
                          "all_previously_audited_months_excluded": True}, indent=2))
        return
    if not args.approve_cloud:
        raise RuntimeError("Cloud access requires --approve-cloud")
    read = os.getenv("HF_READ_TOKEN", "").strip()
    write = os.getenv("HF_WRITE_TOKEN", "").strip()
    if not read or not write:
        raise RuntimeError("HF_READ_TOKEN/HF_WRITE_TOKEN required")
    cloud = PrivateBucket(args.bucket, read, write)
    if args.command == "adopt":
        if not args.month or not args.request_id or args.month not in months:
            raise RuntimeError("--month and --request-id within reviewed range required")
        adopt_one(cloud, args.month, args.request_id)
        return
    names = cloud.names(PREFIX + "/")
    pending = []
    incomplete = []
    for m in months:
        state = cloud.state(m, names)
        if state is None:
            incomplete.append(m)
        elif state.get("status") == "PASS":
            if raw_path(m) not in names or manifest_path(m) not in names:
                raise RuntimeError(f"Cloud PASS lacks Raw/manifest: {m}")
            print(f"SKIP PASS {m}")
        else:
            pending.append((m, state))
    if len(pending) > 1:
        raise RuntimeError("Multiple pending months in cloud; refuse to start another job")
    if pending:
        m, s = pending[0]
        if s.get("status") == "HOLD_REJECTED":
            raise RuntimeError(f"Month {m} is rejected/failed; manual review needed")
        progress_one(cloud, m, s, names)
        return
    if not incomplete:
        print("ALL MONTHS PASS; no CDS call")
        return
    if not (args.allow_new_submission and args.approve_api and args.reviewed_cds_request_history):
        print("NO PENDING JOB. New submission blocked; require explicit CDS history review + approvals")
        print("NEXT MONTH:", incomplete[0])
        return
    submit_one(cloud, incomplete[0])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # No secret-bearing exception/traceback from external SDKs in job logs.
        print(f"STOP ({type(exc).__name__}): {str(exc)[:350]}")
        raise SystemExit(2)
