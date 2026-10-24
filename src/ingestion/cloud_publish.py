"""Publish ONE already-audited LIVE acquisition to a private HF Storage Bucket.

Dry-run by default. This does not ingest, schedule, delete, or mutate local Raw.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.common.validation import validate_config
from .backfill import FIRMS_PRODUCTS, ROOT, SOURCES, plan_backfill
from .backfill_audit import audit_item
from .era5 import _paths as era5_paths, validate_bbox, variables_for_request
from .firms import _paths as firms_paths, valid_bbox
from .openaq import _paths as openaq_paths

DEFAULT_PREFIX = "lake/v1"
MAX_TOTAL_BYTES = 16 * 1024 * 1024
MAX_BUCKET_BYTES = 20 * 1024**3  # internal guard, not a provider billing guarantee


@dataclass(frozen=True)
class Entry:
    local: Path
    remote: str
    size: int
    sha256: str
    is_manifest: bool


def _digest(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    length = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            h.update(chunk)
            length += len(chunk)
    return length, h.hexdigest()


def _safe_local(root: Path, relative: str) -> Path:
    if (not isinstance(relative, str) or not relative.startswith("data/raw/")
            or "\\" in relative or ":" in relative or
            any(part in ("", ".", "..") for part in relative.split("/"))):
        raise ValueError("unsafe or non-Raw relative path")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("Raw/manifest file is absent or outside repository")
    return path


def verified_entries(*, root: Path, source: str, region: str, start: str,
                     end: str, sensor_id: int | None, product: str | None,
                     config: dict, prefix: str, max_bytes: int) -> tuple[Entry, ...]:
    """Audit exact request (incl. OpenAQ pagination) before preparing upload."""
    plans = plan_backfill(
        source=source, region_id=region, start=start, end=end,
        sensor_ids=(sensor_id,) if sensor_id is not None else (),
        product=product, max_plan_items=1,
    )
    if len(plans) != 1:
        raise ValueError("one complete acquisition window required")
    item = plans[0]
    audit = audit_item(root=root, item=item, config=config)
    if audit.status != "complete":
        raise ValueError(f"local audit is {audit.status}: {audit.reason}; no cloud upload")

    first = date.fromisoformat(start)
    if source == "openaq":
        _, first_mf = openaq_paths(root, sensor_id, first, 1)
        manifests = sorted(first_mf.parent.glob("page=*.manifest.json"))
    elif source == "firms":
        bbox = valid_bbox(config["regions"][region])
        days = (date.fromisoformat(end) - first).days
        _, mf, _ = firms_paths(root, product, region, first, days, bbox, "live")
        manifests = [mf]
    else:
        bbox = validate_bbox(config["regions"][region])
        variables = variables_for_request(config["ingestion"]["era5"]["variables"], None)
        _, mf, _ = era5_paths(root, region, first, bbox, variables, "live")
        manifests = [mf]
    if not manifests:
        raise ValueError("COMPLETE audit with no manifests: abort")

    prepared: list[Entry] = []
    total = 0
    for manifest_path in manifests:
        mf_relative = manifest_path.relative_to(root).as_posix()
        mf_file = _safe_local(root, mf_relative)
        m = json.loads(mf_file.read_text(encoding="utf-8"))
        raw_relative = m.get("file")
        raw_file = _safe_local(root, raw_relative)
        if (m.get("acquisition_mode") != "live" or m.get("status") not in ("success", "empty")
                or m.get("region_id") != region or
                m.get("start_utc") != f"{start}T00:00:00Z" or
                m.get("end_utc") != f"{end}T00:00:00Z"):
            raise ValueError("manifest provenance/window mismatch")
        raw_size, raw_sha = _digest(raw_file)
        if (m.get("bytes") != raw_size or m.get("sha256") != raw_sha):
            raise ValueError("local manifest/raw SHA-256 mismatch")
        mf_size, mf_sha = _digest(mf_file)
        total += raw_size + mf_size
        if total > max_bytes:
            raise ValueError("cloud upload byte budget exceeded")
        prepared.extend((
            Entry(raw_file, f"{prefix}/{raw_relative}", raw_size, raw_sha, False),
            Entry(mf_file, f"{prefix}/{mf_relative}", mf_size, mf_sha, True),
        ))
    names = [e.remote for e in prepared]
    if len(set(names)) != len(names):
        raise ValueError("duplicate remote object paths")
    return tuple(prepared)


def publish(bucket: str, entries: tuple[Entry, ...], *, max_bucket_bytes: int = MAX_BUCKET_BYTES) -> tuple[int, int]:
    # Delayed import: offline preview never reads local Hugging Face credentials.
    from huggingface_hub import bucket_info, list_bucket_tree, batch_bucket_files, download_bucket_files

    info = bucket_info(bucket)
    if info.private is not True:
        raise RuntimeError("bucket is not PRIVATE; no upload")
    remote = {
        f.path: f for f in list_bucket_tree(bucket, prefix=entries[0].remote.rsplit("/data/raw/", 1)[0] + "/", recursive=True)
        if getattr(f, "type", "file") == "file"
    }
    wanted = {e.remote: e for e in entries}
    if len(wanted) != len(entries):
        raise RuntimeError("duplicate cloud keys")
    added_bytes = sum(e.size for e in entries if e.remote not in remote)
    if int(info.size) + added_bytes > max_bucket_bytes:
        raise RuntimeError("bucket storage safety cap reached; no upload")
    # A visible manifest with missing Raw is an inconsistent remote publication.
    for i in range(0, len(entries), 2):
        raw, mf = entries[i:i+2]
        if mf.remote in remote and raw.remote not in remote:
            raise RuntimeError("remote manifest exists without Raw; HOLD for review")

    with tempfile.TemporaryDirectory(prefix="hf-cloud-verified-") as tmp:
        tmp_root = Path(tmp)

        def verify(objects: list[Entry]) -> None:
            if not objects:
                return
            pairs = []
            for index, entry in enumerate(objects):
                out = tmp_root / f"obj-{index:05d}"
                pairs.append((entry.remote, str(out)))
            for index in range(0, len(pairs), 12):
                download_bucket_files(bucket, files=pairs[index:index + 12])
            for index, entry in enumerate(objects):
                if _digest(tmp_root / f"obj-{index:05d}") != (entry.size, entry.sha256):
                    raise RuntimeError("remote object differs from local SHA-256; STOP")

        already = [e for e in entries if e.remote in remote]
        verify(already)  # refuse to overwrite any differing remote object
        new_raw = [e for e in entries if e.remote not in remote and not e.is_manifest]
        new_mf = [e for e in entries if e.remote not in remote and e.is_manifest]
        for phase in (new_raw, new_mf):
            for i in range(0, len(phase), 8):
                chunk = phase[i:i+8]
                batch_bucket_files(bucket, add=[(str(e.local), e.remote) for e in chunk])
                verify(chunk)
        return len(new_raw) + len(new_mf), len(already)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default="CourseworkBigData/env-event-lake")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--source", choices=SOURCES, required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--sensor-id", type=int)
    parser.add_argument("--product", choices=FIRMS_PRODUCTS)
    parser.add_argument("--config", default="configs/project.yaml")
    parser.add_argument("--max-bytes", type=int, default=MAX_TOTAL_BYTES)
    parser.add_argument("--approve-upload", action="store_true", help="Actually upload to private bucket")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", args.prefix) or any(
        x in ("", ".", "..") for x in args.prefix.split("/")
    ):
        parser.error("invalid cloud prefix")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", args.bucket):
        parser.error("bucket must be owner/name")
    if not 0 < args.max_bytes <= 128 * 1024 * 1024:
        parser.error("max-bytes must be 1..134217728")
    try:
        root = ROOT
        conf_path = Path(args.config)
        if not conf_path.is_absolute():
            conf_path = root / conf_path
        config = validate_config(conf_path)
        if args.region not in config["regions"]:
            raise ValueError("unknown configured region")
        entries = verified_entries(
            root=root, source=args.source, region=args.region,
            start=args.start, end=args.end, sensor_id=args.sensor_id,
            product=args.product, config=config, prefix=args.prefix,
            max_bytes=args.max_bytes,
        )
        print(f"CLOUD PUBLISH PLAN: VERIFIED ({len(entries)} objects, {sum(e.size for e in entries)} bytes)")
        print(f"BUCKET: {args.bucket}; PREFIX: {args.prefix}/")
        if not args.approve_upload:
            print("DRY RUN ONLY — no cloud calls, token reads, Raw writes, or deletions")
            return 0
        added, reused = publish(args.bucket, entries)
        print(f"CLOUD PUBLISH: PASS (new={added}, reused_verified={reused})")
        print("NO LOCAL RAW WRITES OR CLOUD DELETIONS")
        return 0
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
        print(f"CLOUD PUBLISH: STOP — {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"CLOUD PUBLISH: STOP — {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
