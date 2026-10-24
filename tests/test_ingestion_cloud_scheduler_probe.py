"""Offline contract tests for the read-only HF cloud inventory pilot."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.ingestion.cloud_scheduler_probe import inventory, main


class CloudInventoryTests(unittest.TestCase):
    def setUp(self):
        self.bucket = "CourseworkBigData/env-event-lake"
        self.prefix = "lake/v1"
        self.raw = "data/raw/era5/a.grib"
        self.manifest = "data/raw/era5/manifests/a.manifest.json"
        self.payload = b"GRIB test data"
        self.doc = {
            "file": self.raw, "bytes": len(self.payload),
            "sha256": hashlib.sha256(self.payload).hexdigest(),
            "status": "success",
        }
        self.mf_bytes = json.dumps(self.doc).encode()
        self.nodes = [
            SimpleNamespace(type="file", path=f"lake/v1/{self.raw}", size=len(self.payload)),
            SimpleNamespace(type="file", path=f"lake/v1/{self.manifest}", size=len(self.mf_bytes)),
        ]
        self.info = SimpleNamespace(private=True)
        self.calls = []

    def call(self, nodes=None, doc_bytes=None, info=None):
        entries = self.nodes if nodes is None else nodes
        contents = self.mf_bytes if doc_bytes is None else doc_bytes
        bucket_info = self.info if info is None else info

        def get_info(bucket, *, token):
            self.calls.append("info")
            return bucket_info

        def list_tree(bucket, *, prefix, recursive, token):
            self.calls.append("list")
            return iter(entries)

        def download(bucket, *, files, token, raise_on_missing_files):
            self.calls.append("download")
            for _, name in files:
                Path(name).write_bytes(contents)

        return inventory(self.bucket, self.prefix, token="test-token",
                         get_info=get_info, list_tree=list_tree, download=download)

    def test_valid_pair(self):
        result = self.call()
        self.assertEqual((result.raw, result.manifests, result.orphan_raw), (1, 1, 0))
        self.assertEqual(self.calls, ["info", "list", "download"])

    def test_refuse_public_bucket_before_listing(self):
        with self.assertRaises(ValueError):
            self.call(info=SimpleNamespace(private=False))
        self.assertEqual(self.calls, ["info"])

    def test_refuse_manifest_without_raw(self):
        with self.assertRaisesRegex(ValueError, "missing Raw"):
            self.call(nodes=self.nodes[1:])

    def test_refuse_raw_without_manifest(self):
        with self.assertRaisesRegex(ValueError, "Raw without manifest"):
            self.call(nodes=self.nodes[:1])

    def test_refuse_wrong_metadata_size(self):
        changed = dict(self.doc, bytes=3)
        blob = json.dumps(changed).encode()
        nodes = [self.nodes[0], SimpleNamespace(type="file", path=self.nodes[1].path, size=len(blob))]
        with self.assertRaisesRegex(ValueError, "size/digest"):
            self.call(nodes=nodes, doc_bytes=blob)

    def test_refuse_path_traversal(self):
        changed = dict(self.doc, file="data/raw/../../etc/passwd")
        blob = json.dumps(changed).encode()
        nodes = [self.nodes[0], SimpleNamespace(type="file", path=self.nodes[1].path, size=len(blob))]
        with self.assertRaisesRegex(ValueError, "unsafe"):
            self.call(nodes=nodes, doc_bytes=blob)

    def test_refuse_malformed_sha(self):
        changed = dict(self.doc, sha256="wrong")
        blob = json.dumps(changed).encode()
        nodes = [self.nodes[0], SimpleNamespace(type="file", path=self.nodes[1].path, size=len(blob))]
        with self.assertRaisesRegex(ValueError, "size/digest"):
            self.call(nodes=nodes, doc_bytes=blob)

    def test_refuse_duplicate_key(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.call(nodes=self.nodes + [self.nodes[0]])

    def test_refuse_invalid_prefix(self):
        with self.assertRaises(ValueError):
            inventory(self.bucket, "../lake", token="x", get_info=lambda *a, **kw: None,
                      list_tree=None, download=None)

    def test_empty_private_prefix(self):
        result = self.call(nodes=[])
        self.assertEqual(result.raw, 0)
        self.assertEqual(result.manifests, 0)
        self.assertEqual(self.calls, ["info", "list"])

    def test_no_token_skips_without_cloud_calls(self):
        from unittest.mock import patch
        with patch.dict("os.environ", {"HF_TOKEN": ""}):
            self.assertEqual(main([]), 0)


if __name__ == "__main__":
    unittest.main()
