import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "bulk" / "era5_hf_staging_verify.py"
spec = importlib.util.spec_from_file_location("verifier", SCRIPT)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class VerifyTests(unittest.TestCase):
    def test_fields_and_guard(self):
        self.assertEqual(verifier.expected_fields("2023-08"), 3720)
        self.assertEqual(verifier.expected_fields("2023-09"), 3600)
        with self.assertRaises(ValueError):
            verifier.parse_months("2023-08,2023-08")
        with self.assertRaises(ValueError):
            verifier.parse_months("2023-07")

    def test_valid_fake_month_without_network(self):
        m = "2023-08"
        sha = "a" * 64
        rid = "31c4196c-49da-4259-be27-29c4f295ffb9"
        reqsha = "b" * 64
        state = dict(month=m, dataset=verifier.DATASET, expected_fields=3720,
                     audit_status="PASS", status="PASS", size_bytes=20,
                     sha256=sha, request_id=rid, request_sha256=verifier.expected_request_sha(m),
                     request=verifier.expected_request(m),
                     audited_grib_messages=3720, unique_date_hour_parameter=3720,
                     grid_dimensions="33x65")
        manifest = dict(state, bbox_nwse=verifier.BBOX_NWSE, data_format="grib",
                        cloud_prefix=verifier.PREFIX, variable=list(verifier.VARIABLES),
                        audit_method="ecCodes date/hour/parameter/grid + SHA-256")
        out = verifier.reconcile_month(m, state, manifest, 20, sha)
        self.assertEqual(out["status"], "VERIFIED_READ_ONLY")
        self.assertEqual(out["canonical_status"], "NOT_PROMOTED_OR_VALIDATED")
        with self.assertRaisesRegex(ValueError, "bytes/SHA mismatch"):
            verifier.reconcile_month(m, state, manifest, 19, sha)
        manifest["bbox_nwse"] = [1, 2, 3, 4]
        with self.assertRaisesRegex(ValueError, "bounding box mismatch"):
            verifier.reconcile_month(m, state, manifest, 20, sha)

    def test_fake_hf_download_and_private_guard(self):
        m = "2023-08"
        raw = b"GRIB" * 20
        from hashlib import sha256
        sha = sha256(raw).hexdigest()
        common = dict(month=m, dataset=verifier.DATASET, expected_fields=3720,
                      audit_status="PASS", size_bytes=len(raw), sha256=sha,
                      request_id="31c4196c-49da-4259-be27-29c4f295ffb9",
                      request_sha256=verifier.expected_request_sha(m),
                      request=verifier.expected_request(m), audited_grib_messages=3720,
                      unique_date_hour_parameter=3720, grid_dimensions="33x65")
        state = dict(common, status="PASS")
        manifest = dict(common, bbox_nwse=verifier.BBOX_NWSE,
                        data_format="grib", cloud_prefix=verifier.PREFIX,
                        variable=list(verifier.VARIABLES),
                        audit_method="ecCodes date/hour/parameter/grid + SHA-256")
        mapping = {
            verifier.key_for(m, "state"): json.dumps(state).encode(),
            verifier.key_for(m, "manifest"): json.dumps(manifest).encode(),
            verifier.key_for(m, "raw"): raw,
        }
        calls = []
        def info(bucket, token):
            calls.append("info")
            return SimpleNamespace(private=True)
        def listing(bucket, prefix, recursive, token):
            return [SimpleNamespace(path=k, type="file") for k in mapping]
        def download(bucket, files, token, raise_on_missing_files):
            for key, local in files:
                Path(local).write_bytes(mapping[key])
        out = verifier.verify_remote((m,), bucket=verifier.BUCKET, token="unit-test-token",
                                     get_info=info, list_tree=listing, download=download)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["sha256"], sha)
        self.assertEqual(calls, ["info"])
        def public_info(bucket, token):
            return SimpleNamespace(private=False)
        with self.assertRaisesRegex(ValueError, "not private"):
            verifier.verify_remote((m,), bucket=verifier.BUCKET, token="unit-test-token",
                                   get_info=public_info, list_tree=listing, download=download)

if __name__ == "__main__":
    unittest.main()
