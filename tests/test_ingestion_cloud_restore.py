"""Contract checks for bounded, verified, idempotent cloud-to-runner Raw restoration."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from src.ingestion.cloud_restore import restore, main


class CloudRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bucket = "CourseworkBigData/env-event-lake"
        self.prefix = "lake/v1"
        self.raw_rel = "data/raw/era5/reanalysis-era5-single-levels/HANOI/year=2024/month=11/day=05/live-test.grib"
        self.mf_rel = "data/raw/era5/manifests/reanalysis-era5-single-levels/HANOI/year=2024/month=11/day=05/live-test.manifest.json"
        self.raw_key = self.prefix + "/" + self.raw_rel
        self.mf_key = self.prefix + "/" + self.mf_rel
        self.content = b"GRIB\x00 example"
        self.doc = {
            "schema_version": "manifest-v1", "status": "success", "acquisition_mode": "live",
            "file": self.raw_rel, "bytes": len(self.content),
            "sha256": hashlib.sha256(self.content).hexdigest(),
        }
        self.files = {self.raw_key: self.content, self.mf_key: json.dumps(self.doc).encode()}
        self.calls = []
        self.private = True
        self.bad_metadata = False
        self.fail_raw_download = False

    def run_restore(self, *, max_bytes=8 * 1024 * 1024, token="token-not-real", scope_source=None, scope_day=None):
        def get_info(bucket, *, token):
            self.calls.append("info")
            return SimpleNamespace(private=self.private)

        def list_tree(bucket, *, prefix, recursive, token):
            self.calls.append("list")
            nodes = [SimpleNamespace(type="file", path=key, size=len(blob))
                     for key, blob in self.files.items()]
            if self.bad_metadata:
                nodes[0].size += 1
            return nodes

        def download(bucket, *, files, token, raise_on_missing_files):
            self.calls.append("download")
            for key, path in files:
                if self.fail_raw_download and key == self.raw_key:
                    raise OSError("network interrupted")
                Path(path).write_bytes(self.files[key])

        return restore(bucket=self.bucket, prefix=self.prefix, root=self.root,
                       token=token, max_bytes=max_bytes,
                       get_info=get_info, list_tree=list_tree, download=download,
                       scope_source=scope_source, scope_day=scope_day)


    def test_scoped_restore_selected_day_only(self):
        other_key = self.raw_key.replace("day=05", "day=06")
        self.files[other_key] = b"other day, intentionally orphaned"
        result = self.run_restore(scope_source="era5", scope_day="2024-11-05")
        self.assertEqual((result.raw, result.manifests, result.created), (1, 1, 2))
        self.assertTrue((self.root / self.raw_rel).is_file())
        self.assertFalse((self.root / self.raw_rel.replace("day=05", "day=06")).exists())

    def test_scoped_restore_empty_day_is_noop(self):
        result = self.run_restore(
            scope_source="era5", scope_day="2024-11-06"
        )
        self.assertEqual(
            (result.raw, result.manifests, result.created,
             result.reused, result.bytes_verified),
            (0, 0, 0, 0, 0),
        )
        self.assertNotIn("download", self.calls)

    def test_full_restore_still_refuses_empty_inventory(self):
        self.files.clear()
        with self.assertRaisesRegex(ValueError, "empty or incomplete"):
            self.run_restore()
        self.assertNotIn("download", self.calls)

    def test_scoped_firms_restores_month_partition(self):
        raw = (
            "data/raw/nasa_firms/VIIRS_SNPP_NRT/HANOI/"
            "year=2024/month=11/sample.csv"
        )
        manifest = (
            "data/raw/nasa_firms/manifests/VIIRS_SNPP_NRT/HANOI/"
            "year=2024/month=11/sample.manifest.json"
        )
        content = b"example firms raw"
        doc = {
            "schema_version": "manifest-v1",
            "status": "success",
            "acquisition_mode": "live",
            "file": raw,
            "bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }

        other_raw = raw.replace("month=11", "month=12")
        other_manifest = manifest.replace("month=11", "month=12")
        other_doc = dict(doc, file=other_raw)

        self.files = {
            self.prefix + "/" + raw: content,
            self.prefix + "/" + manifest: json.dumps(doc).encode(),
            self.prefix + "/" + other_raw: content,
            self.prefix + "/" + other_manifest: json.dumps(other_doc).encode(),
        }

        result = self.run_restore(
            scope_source="firms", scope_day="2024-11-05"
        )

        self.assertEqual(
            (result.raw, result.manifests, result.created), (1, 1, 2)
        )
        self.assertTrue((self.root / raw).is_file())
        self.assertTrue((self.root / manifest).is_file())
        self.assertFalse((self.root / other_raw).exists())

    def test_scoped_firms_refuses_orphan_in_selected_month(self):
        raw = (
            "data/raw/nasa_firms/VIIRS_SNPP_NRT/HANOI/"
            "year=2024/month=11/orphan.csv"
        )
        self.files = {self.prefix + "/" + raw: b"orphan"}

        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.run_restore(
                scope_source="firms", scope_day="2024-11-05"
            )
        self.assertNotIn("download", self.calls)

    def test_scoped_restore_refuses_missing_manifest(self):
        del self.files[self.mf_key]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.run_restore(scope_source="era5", scope_day="2024-11-05")
        self.assertNotIn("download", self.calls)
        self.assertFalse((self.root / self.raw_rel).exists())

    def test_scoped_restore_refuses_invalid_scope(self):
        with self.assertRaisesRegex(ValueError, "source and day"):
            self.run_restore(scope_source="era5")
        with self.assertRaisesRegex(ValueError, "invalid scope day"):
            self.run_restore(scope_source="era5", scope_day="2024-11-31")
        with self.assertRaisesRegex(ValueError, "invalid scope source"):
            self.run_restore(scope_source="unknown", scope_day="2024-11-05")

    def test_cli_forwards_scope_to_restore(self):
        fake_hub = ModuleType("huggingface_hub")
        for name in ("bucket_info", "list_bucket_tree", "download_bucket_files"):
            setattr(fake_hub, name, lambda *args, **kwargs: None)

        result = SimpleNamespace(
            raw=0, manifests=0, created=0, reused=0, bytes_verified=0
        )

        with (
            patch.dict("sys.modules", {"huggingface_hub": fake_hub}),
            patch.dict("os.environ", {"HF_TOKEN": "test-token"}),
            patch("src.ingestion.cloud_restore.restore",
                  return_value=result) as mocked,
        ):
            code = main([
                "--root", str(self.root),
                "--scope-source", "era5",
                "--scope-day", "2024-11-05",
                "--approve-download",
            ])

        self.assertEqual(code, 0)
        self.assertEqual(mocked.call_args.kwargs["scope_source"], "era5")
        self.assertEqual(mocked.call_args.kwargs["scope_day"], "2024-11-05")

    def test_restore_pair_fresh(self):
        result = self.run_restore()
        self.assertEqual((result.raw, result.manifests, result.created, result.reused), (1, 1, 2, 0))
        self.assertEqual((self.root / self.raw_rel).read_bytes(), self.content)
        self.assertEqual((self.root / self.mf_rel).read_bytes(), self.files[self.mf_key])

    def test_retry_existing_matches_without_changes(self):
        self.run_restore()
        result = self.run_restore()
        self.assertEqual((result.created, result.reused), (0, 2))

    def test_refuse_local_conflict_before_any_new_write(self):
        raw = self.root / self.raw_rel
        raw.parent.mkdir(parents=True)
        raw.write_bytes(b"my separate data")
        with self.assertRaisesRegex(ValueError, "no overwrite"):
            self.run_restore()
        self.assertEqual(raw.read_bytes(), b"my separate data")
        self.assertFalse((self.root / self.mf_rel).exists())

    def test_refuse_corrupt_raw_without_writes(self):
        self.files[self.raw_key] = b"CORRUPTED DATA"
        self.doc["bytes"] = len(self.files[self.raw_key])
        self.files[self.mf_key] = json.dumps(self.doc).encode()
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.run_restore()
        self.assertFalse((self.root / self.raw_rel).exists())

    def test_refuse_manifest_wrong_size(self):
        self.doc["bytes"] = 1
        self.files[self.mf_key] = json.dumps(self.doc).encode()
        with self.assertRaisesRegex(ValueError, "metadata"):
            self.run_restore()

    def test_refuse_cloud_orphan(self):
        del self.files[self.mf_key]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.run_restore()

    def test_refuse_public_bucket(self):
        self.private = False
        with self.assertRaisesRegex(ValueError, "private"):
            self.run_restore()
        self.assertEqual(self.calls, ["info"])

    def test_refuse_too_much_data_before_download(self):
        with self.assertRaisesRegex(ValueError, "budget"):
            self.run_restore(max_bytes=1)
        self.assertNotIn("download", self.calls)

    def test_refuse_path_traversal_manifest(self):
        self.doc["file"] = "data/raw/../../.github/workflows/x"
        self.files[self.mf_key] = json.dumps(self.doc).encode()
        with self.assertRaisesRegex(ValueError, "path"):
            self.run_restore()

    def test_refuse_symlink_in_local_path(self):
        with tempfile.TemporaryDirectory() as elsewhere:
            try:
                (self.root / "data").symlink_to(elsewhere, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("Windows symlink creation requires developer mode or privileges")
            with self.assertRaisesRegex(ValueError, "symlink"):
                self.run_restore()

    def test_network_failure_creates_no_raw(self):
        self.fail_raw_download = True
        with self.assertRaises(OSError):
            self.run_restore()
        self.assertFalse((self.root / self.raw_rel).exists())

    def test_missing_token(self):
        with self.assertRaisesRegex(ValueError, "token"):
            self.run_restore(token="")
        self.assertEqual(self.calls, [])

    def test_default_cli_offline_does_not_read_token_or_network(self):
        with patch.dict("os.environ", {"HF_TOKEN": ""}):
            self.assertEqual(main([]), 0)

    def test_wrong_cloud_metadata_bytes(self):
        self.bad_metadata = True
        with self.assertRaises(ValueError):
            self.run_restore()
        self.assertFalse((self.root / self.raw_rel).exists())


if __name__ == "__main__":
    unittest.main()
