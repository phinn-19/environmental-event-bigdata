"""Offline tests for safe cloud Raw/Manifest publisher (no real Hub calls)."""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from src.ingestion.cloud_publish import main, publish, verified_entries
from src.ingestion.manifest import make_manifest, request_id
from src.ingestion.openaq import _paths as openaq_paths
from src.common.validation import validate_config


class MockBucket:
    def __init__(self):
        self.items = {}
        self.private = True
        self.uploads = 0

    def api(self):
        outer = self

        def bucket_info(bucket):
            return types.SimpleNamespace(private=outer.private, size=sum(map(len, outer.items.values())))

        def list_bucket_tree(bucket, prefix="", recursive=False):
            return [types.SimpleNamespace(path=k, type="file") for k in outer.items if k.startswith(prefix)]

        def batch_bucket_files(bucket, add):
            for src, dest in add:
                outer.items[dest] = Path(src).read_bytes()
                outer.uploads += 1

        def download_bucket_files(bucket, files):
            for remote, out in files:
                dst = Path(out)
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(outer.items[remote])

        return types.SimpleNamespace(
            bucket_info=bucket_info,
            list_bucket_tree=list_bucket_tree,
            batch_bucket_files=batch_bucket_files,
            download_bucket_files=download_bucket_files,
        )


class CloudPublisherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = validate_config(Path(__file__).resolve().parents[1] / "configs/project.yaml")
        day = date(2024, 11, 5)
        self.raw, self.mf = openaq_paths(self.root, 21632, day, 1)
        self.raw.parent.mkdir(parents=True, exist_ok=True)
        self.mf.parent.mkdir(parents=True, exist_ok=True)
        payload = {"results": [{"value": 12}], "meta": {"found": 1}}
        content = json.dumps(payload).encode()
        self.raw.write_bytes(content)
        manifest = make_manifest(
            region_id="HANOI", start_utc="2024-11-05T00:00:00Z",
            end_utc="2024-11-06T00:00:00Z",
            request_id_value=request_id("openaq", 21632, "2024-11-05", 1),
            mode="live", status="success", raw_file=self.raw.relative_to(self.root).as_posix(),
            raw_bytes=content, row_count=1,
        )
        self.mf.write_text(json.dumps(manifest), encoding="utf-8")

    def plan(self):
        return verified_entries(
            root=self.root, source="openaq", region="HANOI", start="2024-11-05",
            end="2024-11-06", sensor_id=21632, product=None,
            config=self.config, prefix="lake/v1", max_bytes=20000,
        )

    def test_offline_plan_returns_raw_then_manifest_without_cloud(self):
        entries = self.plan()
        self.assertEqual(len(entries), 2)
        self.assertFalse(entries[0].is_manifest)
        self.assertTrue(entries[1].is_manifest)
        self.assertEqual(entries[0].sha256, hashlib.sha256(self.raw.read_bytes()).hexdigest())

    def test_corrupted_local_raw_is_rejected(self):
        self.raw.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "local audit"):
            self.plan()

    def test_private_upload_readback_and_repeat_skip(self):
        backend = MockBucket()
        entries = self.plan()
        with patch.dict(sys.modules, {"huggingface_hub": backend.api()}):
            self.assertEqual(publish("org/bucket", entries), (2, 0))
            self.assertEqual(publish("org/bucket", entries), (0, 2))
        self.assertEqual(backend.uploads, 2)

    def test_public_bucket_rejected_before_upload(self):
        backend = MockBucket()
        backend.private = False
        with patch.dict(sys.modules, {"huggingface_hub": backend.api()}):
            with self.assertRaisesRegex(RuntimeError, "not PRIVATE"):
                publish("org/bucket", self.plan())
        self.assertEqual(backend.uploads, 0)

    def test_remote_different_raw_aborts_without_overwriting(self):
        backend = MockBucket()
        entries = self.plan()
        backend.items[entries[0].remote] = b"different"
        with patch.dict(sys.modules, {"huggingface_hub": backend.api()}):
            with self.assertRaisesRegex(RuntimeError, "differs"):
                publish("org/bucket", entries)
        self.assertEqual(backend.uploads, 0)

    def test_orphan_manifest_blocks(self):
        backend = MockBucket()
        entries = self.plan()
        backend.items[entries[1].remote] = entries[1].local.read_bytes()
        with patch.dict(sys.modules, {"huggingface_hub": backend.api()}):
            with self.assertRaisesRegex(RuntimeError, "without Raw"):
                publish("org/bucket", entries)
        self.assertEqual(backend.uploads, 0)

    def test_raw_uploaded_then_manifest_failure_resumes(self):
        backend = MockBucket()
        entries = self.plan()
        api = backend.api()
        original = api.batch_bucket_files
        attempts = {"n": 0}

        def flaky(bucket, add):
            attempts["n"] += 1
            if attempts["n"] == 2:
                raise OSError("simulated network failure before manifest")
            original(bucket, add)

        api.batch_bucket_files = flaky
        with patch.dict(sys.modules, {"huggingface_hub": api}):
            with self.assertRaises(OSError):
                publish("org/bucket", entries)
        self.assertIn(entries[0].remote, backend.items)
        self.assertNotIn(entries[1].remote, backend.items)
        with patch.dict(sys.modules, {"huggingface_hub": backend.api()}):
            self.assertEqual(publish("org/bucket", entries), (1, 1))
        self.assertEqual(backend.uploads, 2)

    def test_hard_byte_limit(self):
        with self.assertRaisesRegex(ValueError, "budget exceeded"):
            verified_entries(
                root=self.root, source="openaq", region="HANOI",
                start="2024-11-05", end="2024-11-06", sensor_id=21632,
                product=None, config=self.config, prefix="lake/v1", max_bytes=10,
            )


if __name__ == "__main__":
    unittest.main()
