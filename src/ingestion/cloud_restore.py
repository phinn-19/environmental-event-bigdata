"""Bounded, verified, read-only HF -> local Raw restoration for ephemeral runners.

Offline by default. With explicit approval, download complete live Raw/Manifest
pairs into an isolated temporary directory, verify all Raw SHA-256 digests,
then create only missing local files (Raw before manifest). Never overwrite.
This is NOT provider ingestion, cloud publishing, or scheduling.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .cloud_scheduler_probe import (
    DEFAULT_BUCKET,
    DEFAULT_PREFIX,
    MAX_MANIFESTS,
    MAX_MANIFEST_BYTES,
    MAX_OBJECTS,
    _prefix_ok,
    _raw_rel_ok,
)

DEFAULT_MAX_BYTES = 8 * 1024 * 1024
HARD_MAX_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class RestoreResult:
    raw: int
    manifests: int
    created: int
    reused: int
    bytes_verified: int


def _digest(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            total += len(block)
            h.update(block)
    return total, h.hexdigest()


def _destination(root: Path, rel: str) -> Path:
    if not _raw_rel_ok(rel):
        raise ValueError("unsafe Raw-relative path")
    current = root
    for part in rel.split("/"):
        current = current / part
        if current.is_symlink():
            raise ValueError("symlink in Raw destination")
    resolved = current.resolve()
    raw_root = (root / "data" / "raw").resolve()
    if not resolved.is_relative_to(raw_root) or resolved == raw_root:
        raise ValueError("destination outside data/raw")
    return current


def restore(
    *, bucket: str, prefix: str, root: Path, token: str,
    max_bytes: int = DEFAULT_MAX_BYTES,
    get_info: Callable, list_tree: Callable, download: Callable,
) -> RestoreResult:
    """Restore verified pairs. HF API calls are read-only; local writes are bounded.

    No mutation of local Raw happens until ALL remote payloads pass verification,
    and no existing files are overwritten. Cloud metadata/manifest remain untrusted.
    """
    if (not re.fullmatch(r"[^/\s]+/[^/\s]+", bucket)
            or not _prefix_ok(prefix)):
        raise ValueError("invalid bucket or prefix")
    if not token or not token.strip():
        raise ValueError("HF read token required")
    if type(max_bytes) is not int or not 1 <= max_bytes <= HARD_MAX_BYTES:
        raise ValueError("invalid byte budget")
    if not root.is_dir() or root.is_symlink():
        raise ValueError("restore root must be an existing non-symlink directory")
    root = root.resolve()
    if get_info(bucket, token=token).private is not True:
        raise ValueError("private bucket required")

    raw_prefix = prefix + "/data/raw/"
    objects = {}
    for node in list_tree(bucket, prefix=raw_prefix, recursive=True, token=token):
        if getattr(node, "type", None) != "file":
            continue
        key, size = getattr(node, "path", None), getattr(node, "size", None)
        if (not isinstance(key, str) or not key.startswith(raw_prefix)
                or not _raw_rel_ok(key[len(prefix) + 1:])
                or type(size) is not int or size < 0):
            raise ValueError("invalid cloud object metadata")
        if key in objects:
            raise ValueError("duplicate cloud object key")
        objects[key] = size
        if len(objects) > MAX_OBJECTS:
            raise ValueError("cloud object limit exceeded")
    manifests = sorted(key for key in objects if key.endswith(".manifest.json"))
    raw_keys = set(objects).difference(manifests)
    if not manifests or len(manifests) > MAX_MANIFESTS or len(raw_keys) != len(manifests):
        raise ValueError("empty or incomplete cloud Raw/Manifest inventory")
    if sum(objects[k] for k in manifests) > MAX_MANIFEST_BYTES:
        raise ValueError("manifest size limit exceeded")
    total_bytes = sum(objects.values())
    if total_bytes > max_bytes:
        raise ValueError("cloud byte budget exceeded before download")
    # Reject ambiguous local destinations before network downloads.
    for key in objects:
        _destination(root, key[len(prefix) + 1:])

    with tempfile.TemporaryDirectory(prefix=".hf-restore-", dir=root) as tmp:
        stage = Path(tmp)
        staged = {key: stage / f"object-{i:05d}" for i, key in enumerate(sorted(objects))}
        for i in range(0, len(manifests), 12):
            batch = manifests[i:i + 12]
            download(bucket, files=[(key, str(staged[key])) for key in batch],
                     token=token, raise_on_missing_files=True)

        raw_checksums: dict[str, str] = {}
        for key in manifests:
            file = staged[key]
            if not file.is_file() or file.stat().st_size != objects[key] or file.stat().st_size > 8192:
                raise ValueError("missing, oversized or inconsistent manifest")
            try:
                doc = json.loads(file.read_text(encoding="utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError("invalid manifest") from exc
            if not isinstance(doc, dict):
                raise ValueError("invalid manifest body")
            rel = doc.get("file")
            if not _raw_rel_ok(rel):
                raise ValueError("invalid manifest Raw path")
            raw_key = prefix + "/" + rel
            if raw_key not in raw_keys or raw_key in raw_checksums:
                raise ValueError("manifest refers to absent or duplicate Raw")
            if (doc.get("schema_version") != "manifest-v1"
                    or doc.get("status") not in ("success", "empty")
                    or doc.get("acquisition_mode") != "live"
                    or type(doc.get("bytes")) is not int
                    or doc["bytes"] != objects[raw_key]
                    or not isinstance(doc.get("sha256"), str)
                    or re.fullmatch(r"[0-9a-f]{64}", doc["sha256"]) is None):
                raise ValueError("invalid manifest completeness metadata")
            raw_checksums[raw_key] = doc["sha256"]
        if set(raw_checksums) != raw_keys:
            raise ValueError("orphan cloud Raw without manifest")

        raws = sorted(raw_keys)
        for i in range(0, len(raws), 8):
            batch = raws[i:i + 8]
            download(bucket, files=[(key, str(staged[key])) for key in batch],
                     token=token, raise_on_missing_files=True)
        for key in raws:
            if not staged[key].is_file() or _digest(staged[key]) != (objects[key], raw_checksums[key]):
                raise ValueError("remote Raw SHA-256 mismatch")
        for key in manifests:
            if _digest(staged[key])[0] != objects[key]:
                raise ValueError("manifest changed during download")

        # Fail all local conflicts before any local publication.
        expected = {key: _digest(staged[key]) for key in objects}
        reused = 0
        for key in sorted(objects):
            dest = _destination(root, key[len(prefix) + 1:])
            if dest.exists():
                if not dest.is_file() or _digest(dest) != expected[key]:
                    raise ValueError("existing local Raw/Manifest differs: no overwrite")
                reused += 1
        created = 0
        for key in raws + manifests:
            dest = _destination(root, key[len(prefix) + 1:])
            if dest.exists():
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            # Hard-link temp payload into place atomically; no replace/overwrite.
            # The stage lives inside root, ensuring it is on the same volume.
            dest = _destination(root, key[len(prefix) + 1:])
            try:
                os.link(staged[key], dest)
                created += 1
            except FileExistsError:
                if not dest.is_file() or _digest(dest) != expected[key]:
                    raise ValueError("concurrent local conflict: no overwrite")
                reused += 1
        return RestoreResult(len(raws), len(manifests), created, reused, total_bytes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--root", default=".")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--approve-download", action="store_true",
                        help="Authorize READ-ONLY remote access and VERIFIED local Raw creation")
    args = parser.parse_args(argv)
    if not args.approve_download:
        print("CLOUD RESTORE: DRY RUN (offline; no token reads, cloud calls or Raw writes)")
        return 0
    token = os.environ.get("HF_TOKEN", "")
    try:
        from huggingface_hub import bucket_info, list_bucket_tree, download_bucket_files
        result = restore(bucket=args.bucket, prefix=args.prefix, root=Path(args.root),
                         token=token, max_bytes=args.max_bytes,
                         get_info=bucket_info, list_tree=list_bucket_tree,
                         download=download_bucket_files)
    except Exception as exc:
        # Never print remote provider payloads, secrets or filesystem paths.
        print(f"CLOUD RESTORE: STOP ({type(exc).__name__})", file=sys.stderr)
        return 2
    print(f"CLOUD RESTORE: PASS (raw={result.raw}, manifests={result.manifests}, "
          f"created={result.created}, reused={result.reused}, bytes_verified={result.bytes_verified})")
    print("RAW SHA-256: PASS; READ ONLY CLOUD; NO PROVIDER API; NO OVERWRITES OR DELETES")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
