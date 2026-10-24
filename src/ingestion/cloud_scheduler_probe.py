"""Read-only Hugging Face Raw/Manifest inventory for a GitHub Actions pilot.

This does NOT restore local Raw, prove raw byte SHA-256, decide provider freshness,
or authorize ingestion. No uploads/deletions are possible in this module.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

DEFAULT_BUCKET = "CourseworkBigData/env-event-lake"
DEFAULT_PREFIX = "lake/v1"
MAX_OBJECTS = 1000
MAX_MANIFESTS = 250
MAX_MANIFEST_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class CloudInventory:
    raw: int
    manifests: int
    orphan_raw: int
    bytes_indexed: int


def _prefix_ok(prefix: str) -> bool:
    return (bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", prefix))
            and all(p not in ("", ".", "..") for p in prefix.split("/")))


def _raw_rel_ok(path: str) -> bool:
    return (isinstance(path, str) and path.startswith("data/raw/")
            and "\\" not in path and ":" not in path
            and all(p not in ("", ".", "..") for p in path.split("/")))


def inventory(bucket: str, prefix: str, *, token: str,
              get_info: Callable, list_tree: Callable,
              download: Callable) -> CloudInventory:
    """Cross-check remote manifest metadata against listed Raw keys and sizes.

    Network I/O is read-only. Content SHA-256 of large Raw objects is NOT
    checked. Existing Sprint 09 publisher remains responsible for full readback.
    """
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", bucket) or not _prefix_ok(prefix):
        raise ValueError("invalid bucket/prefix")
    if not token or not token.strip():
        raise ValueError("explicit HF token required")
    info = get_info(bucket, token=token)
    if info.private is not True:
        raise ValueError("bucket is not private")
    raw_prefix = prefix.rstrip("/") + "/data/raw/"
    objects: dict[str, object] = {}
    for node in list_tree(bucket, prefix=raw_prefix, recursive=True, token=token):
        if getattr(node, "type", None) != "file":
            continue
        key = getattr(node, "path", "")
        size = getattr(node, "size", None)
        if (not isinstance(key, str) or not key.startswith(raw_prefix)
                or not isinstance(size, int) or isinstance(size, bool) or size < 0):
            raise ValueError("unexpected cloud object metadata")
        if key in objects:
            raise ValueError("duplicate remote keys")
        objects[key] = node
        if len(objects) > MAX_OBJECTS:
            raise ValueError("inventory object limit exceeded")

    manifest_keys = sorted(k for k in objects if k.endswith(".manifest.json"))
    raw_keys = set(objects) - set(manifest_keys)
    if len(manifest_keys) > MAX_MANIFESTS:
        raise ValueError("inventory manifest limit exceeded")
    if sum(objects[k].size for k in manifest_keys) > MAX_MANIFEST_BYTES:
        raise ValueError("inventory manifest byte limit exceeded")

    seen_raw = set()
    with tempfile.TemporaryDirectory(prefix="hf-readonly-inventory-") as temp:
        tmp = Path(temp)
        for batch_start in range(0, len(manifest_keys), 20):
            batch = manifest_keys[batch_start:batch_start + 20]
            pairs = [(k, tmp / f"m-{j:04d}.json") for j, k in enumerate(batch, batch_start)]
            download(bucket, files=[(k, str(p)) for k, p in pairs],
                     token=token, raise_on_missing_files=True)
            for remote_path, local_path in pairs:
                if not local_path.is_file() or local_path.stat().st_size != objects[remote_path].size:
                    raise ValueError("manifest missing or wrong byte size")
                if local_path.stat().st_size > 8192:
                    raise ValueError("oversized manifest")
                doc = json.loads(local_path.read_text(encoding="utf-8"))
                if not isinstance(doc, dict):
                    raise ValueError("invalid manifest JSON")
                relative = doc.get("file")
                if not _raw_rel_ok(relative):
                    raise ValueError("unsafe or absent Raw path in manifest")
                raw_key = prefix + "/" + relative
                if raw_key not in raw_keys:
                    raise ValueError("manifest points to missing Raw object")
                if raw_key in seen_raw:
                    raise ValueError("multiple manifests point to one Raw object")
                if (type(doc.get("bytes")) is not int
                        or doc["bytes"] != objects[raw_key].size
                        or not re.fullmatch(r"[0-9a-f]{64}", str(doc.get("sha256", "")))):
                    raise ValueError("manifest size/digest metadata invalid")
                if doc.get("status") not in ("success", "empty"):
                    raise ValueError("manifest has non-complete status")
                seen_raw.add(raw_key)

    orphan = raw_keys - seen_raw
    if orphan:
        raise ValueError("Raw without manifest: incomplete remote publication")
    return CloudInventory(len(raw_keys), len(manifest_keys), len(orphan),
                          sum(node.size for node in objects.values()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    args = parser.parse_args(argv)
    token = os.environ.get("HF_TOKEN", "")
    if not token:
        print("CLOUD INVENTORY: SKIPPED (HF_TOKEN not configured; cloud not checked)")
        return 0
    try:
        from huggingface_hub import bucket_info, list_bucket_tree, download_bucket_files
        result = inventory(args.bucket, args.prefix, token=token,
                           get_info=bucket_info, list_tree=list_bucket_tree,
                           download=download_bucket_files)
    except Exception as exc:
        # Avoid printing tokens, full URLs, provider exception payloads or paths.
        print(f"CLOUD INVENTORY: STOP ({type(exc).__name__})", file=sys.stderr)
        return 2
    print(f"CLOUD INVENTORY: PASS (raw={result.raw}, manifests={result.manifests}, "
          f"orphan_raw={result.orphan_raw}, indexed_bytes={result.bytes_indexed})")
    print("READ ONLY — no uploads, deletes, provider API calls, or local Raw writes")
    print("LIMITATION — Raw SHA-256 and cloud-to-local restoration not validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
