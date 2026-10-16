"""Independent tests for multi-page reports, checksum and JSON/CSV export."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.ingestion.coverage import build_coverage_report, export_coverage_report, load_min_valid_samples


class CoverageExportTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.day = date(2024, 10, 23)
        self.manifests = self.root / "data/raw/openaq/manifests/sensor=21632"
        self.manifests.mkdir(parents=True)
        self.pages = self.root / "data/raw/openaq/hours/sensor=21632"
        self.pages.mkdir(parents=True)

    def add_page(self, n, hours):
        records = [{"period": {"datetimeFrom": {"utc": f"2024-10-23T{h:02d}:00:00Z"}},
                    "value": 35.0, "flagInfo": {"hasFlags": False}} for h in hours]
        raw = self.pages / f"page={n:04d}.json"
        raw.write_text(json.dumps({"results": records}), encoding="utf-8")
        digest = hashlib.sha256(raw.read_bytes()).hexdigest()
        manifest = {"source": "openaq", "acquisition_mode": "live",
                    "start_utc": "2024-10-23T00:00:00Z", "status": "success",
                    "file": raw.relative_to(self.root).as_posix(),
                    "sha256": digest, "row_count": len(records)}
        (self.manifests / f"page={n:04d}.manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return raw

    def test_two_pages_combine_to_one_valid_day(self):
        self.add_page(1, range(12))
        self.add_page(2, range(12, 24))
        report = build_coverage_report(21632, self.day, date(2024, 10, 24),
                                       root=self.root, min_valid_samples=1)
        self.assertEqual(report["verified_days"], 1)
        self.assertEqual(report["manifest_raw_rows"], 24)
        self.assertEqual(report["daily"][0]["pages"], 2)
        self.assertEqual(report["hours_meeting_minimum"], 24)
        self.assertFalse(report["technical_problems"])

        out_dir = self.root / "data/reports"
        json_file, csv_file = export_coverage_report(report, out_dir)
        self.assertEqual(json.loads(json_file.read_text(encoding="utf-8"))["verified_days"], 1)
        self.assertIn("2024-10-23", csv_file.read_text(encoding="utf-8-sig"))

    def test_corrupt_checksum_must_not_be_verified(self):
        raw = self.add_page(1, range(24))
        raw.write_text("tampered", encoding="utf-8")
        report = build_coverage_report(21632, self.day, date(2024, 10, 24),
                                       root=self.root, min_valid_samples=1)
        self.assertEqual(report["verified_days"], 0)
        self.assertEqual(report["manifest_raw_rows"], 0)
        self.assertTrue(any("SHA256 mismatch" in p for p in report["technical_problems"]))

    def test_no_usable_day_is_quality_warning(self):
        raw = self.add_page(1, range(24))
        payload = json.loads(raw.read_text(encoding="utf-8"))
        for record in payload["results"]:
            record["value"] = None
            record["flagInfo"]["hasFlags"] = True
        raw.write_text(json.dumps(payload), encoding="utf-8")
        manifest_file = self.manifests / "page=0001.manifest.json"
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        manifest["sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
        manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
        report = build_coverage_report(21632, self.day, date(2024, 10, 24),
                                       root=self.root, min_valid_samples=1)
        self.assertEqual(report["verified_days"], 1)
        self.assertEqual(report["quality_totals"]["usable"], 0)
        self.assertFalse(report["technical_problems"])
        self.assertIn("NO_USABLE_MEASUREMENTS", report["quality_warnings"][0])

    def test_minimum_reads_from_config(self):
        config = self.root / "project.yaml"
        config.write_text("algorithms:\n  air_quality:\n    baseline:\n      min_valid_samples: 10\n", encoding="utf-8")
        self.assertEqual(load_min_valid_samples(config), 10)

    def test_missing_hour_is_technical_problem(self):
        self.add_page(1, range(23))
        report = build_coverage_report(21632, self.day, date(2024, 10, 24),
                                       root=self.root, min_valid_samples=1)
        self.assertEqual(report["verified_days"], 0)
        self.assertTrue(any("incomplete or duplicate" in p for p in report["technical_problems"]))


if __name__ == "__main__":
    unittest.main()
