"""No-network/no-Spark acceptance checks for bounded HF Processing inputs."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from scripts.processing_hf_select_batch import select
from scripts.processing_hf_package import validate, build, TABLES
from scripts.processing_hf_publish_gate import check
from scripts.processing_hf_scope_outputs import table_days


class BoundedSelectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "repo"
        self.raw = self.root / "data" / "raw"
        self.raw.mkdir(parents=True)
        for name in ("src", "configs", "contracts"):
            (self.root / name).mkdir()
            (self.root / name / "README.txt").write_text("test", encoding="utf-8")
        self.previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.previous)
        self.counter = 0
        self.add("openaq", "openaq_v3_locations", "HANOI", None, None)
        self.add("openaq", "openaq_v3_hours", "HANOI", "2024-10-15", "2024-10-16")
        self.add("openaq", "openaq_v3_hours", "HANOI", "2024-11-01", "2024-11-02")
        self.add("openaq", "openaq_v3_hours", "HANOI", "2024-11-02", "2024-11-03")
        self.add("nasa_firms", "VIIRS_NOAA20_NRT", "VN_BBOX", "2024-11-01", "2024-11-02")
        self.add("nasa_firms", "VIIRS_NOAA20_SP", "VN_BBOX", "2024-11-01", "2024-11-02")
        self.add("era5", "reanalysis-era5-single-levels", "HANOI", "2024-11-01", "2024-11-02")
        self.add("era5", "reanalysis-era5-single-levels", "VN_BBOX", "2024-11-01", "2024-11-02")

    def add(self, source, product, region, start, end, content=None):
        self.counter += 1
        n = self.counter
        ext = {"openaq": "json", "nasa_firms": "csv", "era5": "grib"}[source]
        raw_rel = f"data/raw/{source}/{product}/{region}/{n}.{ext}"
        manifest_rel = f"data/raw/{source}/manifests/{product}/{region}/{n}.manifest.json"
        content = content if content is not None else f"fixture-{n}".encode()
        path = self.root / raw_rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        m = {"schema_version": "manifest-v1", "source": source,
             "source_product": product, "region_id": region,
             "start_utc": start + "T00:00:00Z" if start else None,
             "end_utc": end + "T00:00:00Z" if end else None,
             "status": "success", "format": ext, "file": raw_rel,
             "row_count": 1, "bytes": len(content),
             "sha256": hashlib.sha256(content).hexdigest()}
        mf = self.root / manifest_rel
        mf.parent.mkdir(parents=True, exist_ok=True)
        mf.write_text(json.dumps(m), encoding="utf-8")
        return mf, path

    def pick(self):
        return select(self.raw, self.root / "job", "2024-11-01", "2024-11-01", "2024-11-01", "HANOI")

    def test_bounded_dates_region_and_firms_sp(self):
        report = self.pick()
        self.assertEqual(report["selected_manifests"],
                         {"aq_hours": 2, "aq_locations": 1, "era5": 1, "firms": 1})
        self.assertEqual(report["firms_preferred_product"], "VIIRS_NOAA20_SP")
        paths = list((self.root / "job" / "data" / "raw").rglob("*"))
        self.assertFalse(any("VIIRS_NOAA20_NRT" in str(p) for p in paths))
        self.assertFalse(any("VN_BBOX" in str(p) and "era5" in str(p) for p in paths))
        hour_manifests = (self.root / "job" / "data" / "raw" / "openaq" / "manifests").rglob("*.manifest.json")
        self.assertFalse(any(json.loads(mf.read_text()).get("start_utc") == "2024-11-02T00:00:00Z"
                             for mf in hour_manifests))


    def test_same_region_conflicting_era5_fails_before_writing(self):
        self.add("era5", "reanalysis-era5-single-levels", "HANOI", "2024-11-01", "2024-11-02")
        with self.assertRaisesRegex(ValueError, "Conflicting ERA5"):
            self.pick()
        self.assertFalse((self.root / "job").exists())

    def test_identical_era5_replay_is_collapsed(self):
        original = next((self.raw / "era5" / "reanalysis-era5-single-levels" / "HANOI").glob("*.grib"))
        self.add("era5", "reanalysis-era5-single-levels", "HANOI", "2024-11-01", "2024-11-02", original.read_bytes())
        report = self.pick()
        self.assertEqual(report["selected_manifests"]["era5"], 1)
        self.assertEqual(len(report["excluded_identical_replays"]), 1)

    def test_conflicting_firms_same_product_fails(self):
        self.add("nasa_firms", "VIIRS_NOAA20_SP", "VN_BBOX", "2024-11-01", "2024-11-02")
        with self.assertRaisesRegex(ValueError, "Conflicting FIRMS"):
            self.pick()

    def test_firms_falls_back_to_nrt_when_no_sp(self):
        for mf in (self.raw / "nasa_firms" / "manifests" / "VIIRS_NOAA20_SP").rglob("*.manifest.json"):
            mf.unlink()
        report = self.pick()
        self.assertEqual(report["firms_preferred_product"], "VIIRS_NOAA20_NRT")

    def test_checksum_tampering_fails_without_output(self):
        target = next((self.raw / "era5" / "reanalysis-era5-single-levels" / "HANOI").glob("*.grib"))
        target.write_bytes(b"corrupted")
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            self.pick()
        self.assertFalse((self.root / "job").exists())

    def test_monthly_era5_refused(self):
        for mf in (self.raw / "era5" / "manifests" / "reanalysis-era5-single-levels" / "HANOI").rglob("*.manifest.json"):
            m = json.loads(mf.read_text())
            m["end_utc"] = "2024-12-01T00:00:00Z"
            mf.write_text(json.dumps(m))
        with self.assertRaisesRegex(ValueError, "monthly"):
            self.pick()

    def test_no_overwrite(self):
        (self.root / "job").mkdir()
        with self.assertRaises(FileExistsError):
            self.pick()

    def test_daily_scope_mapping(self):
        values = table_days({"aq_day": "2024-11-01", "firms_day": "2024-11-02", "era5_day": "2024-11-03"})
        self.assertEqual(values["air_quality_baseline"], "2024-11-01")
        self.assertEqual(values["fire_hotspots_context"], "2024-11-02")
        self.assertEqual(values["weather_hourly"], "2024-11-03")
        self.assertEqual(len(values), 6)
        with self.assertRaises(ValueError):
            table_days({"aq_day": "2024-11-31", "firms_day": "2024-11-02", "era5_day": "2024-11-03"})

    def test_package_verifier_rejects_traversal(self):
        package = self.root / "package"
        package.mkdir()
        (package / "SHA256SUMS.txt").write_text("0" * 64 + "  ../escape\n")
        with self.assertRaises(ValueError):
            validate(package)

    def test_publish_gate_blocks_zero_rows(self):
        job, package = self.root / "job", self.root / "package"
        job.mkdir()
        (job / "SELECTION.json").write_text(json.dumps({
            "aq_day": "2024-11-01", "firms_day": "2024-11-01", "era5_day": "2024-11-01"}))
        six = {name: {"rows": 1, "day_utc": "2024-11-01"} for names in TABLES.values() for name in names}
        six["air_quality_hourly"]["rows"] = 0
        six["air_quality_context_hourly"]["weather_matched_rows"] = 1
        six["fire_hotspots_context"]["weather_matched_rows"] = 1
        (job / "SCOPE_REPORT.json").write_text(json.dumps({"tables": six}))
        for tier, names in TABLES.items():
            for name in names:
                target = job / "data" / tier / name
                target.mkdir(parents=True)
                (target / "_SUCCESS").write_bytes(b"")
                (target / "part-000.snappy.parquet").write_bytes(b"fixture")
                (target / ".part-000.snappy.parquet.crc").write_bytes(b"crc")
        build(job, package, "run_gha_test_02", "abc123")
        self.assertFalse(list(package.rglob("*.crc")))
        import csv
        with (package / "INVENTORY.csv").open(newline="") as f:
            inventory = list(csv.DictReader(f))
        self.assertFalse(any(row["path"] == "INVENTORY.csv" for row in inventory))
        self.assertEqual(len(inventory) + 2, validate(package))
        with self.assertRaisesRegex(ValueError, "empty target-day tables"):
            check(package)

    def test_publish_gate_accepts_technical_nonempty_coverage(self):
        job, package = self.root / "job", self.root / "package"
        job.mkdir()
        (job / "SELECTION.json").write_text(json.dumps({
            "aq_day": "2024-11-01", "firms_day": "2024-11-01", "era5_day": "2024-11-01"}))
        six = {name: {"rows": 2, "day_utc": "2024-11-01"} for names in TABLES.values() for name in names}
        six["air_quality_context_hourly"]["weather_matched_rows"] = 1
        six["fire_hotspots_context"]["weather_matched_rows"] = 1
        (job / "SCOPE_REPORT.json").write_text(json.dumps({"tables": six}))
        for tier, names in TABLES.items():
            for name in names:
                target = job / "data" / tier / name
                target.mkdir(parents=True)
                (target / "_SUCCESS").write_bytes(b"")
                (target / "part-000.snappy.parquet").write_bytes(b"fixture")
        build(job, package, "run_gha_test_03", "abc123")
        self.assertEqual(set(check(package)["tables"]), set(six))

    def test_package_build_verifies_scoped_metadata_and_tampering(self):
        job, package = self.root / "job", self.root / "package"
        job.mkdir()
        (job / "SELECTION.json").write_text(json.dumps({"aq_day": "2024-11-01"}))
        six = {name: {"rows": 1} for names in TABLES.values() for name in names}
        (job / "SCOPE_REPORT.json").write_text(json.dumps({"tables": six}))
        for tier, names in TABLES.items():
            for name in names:
                target = job / "data" / tier / name
                target.mkdir(parents=True)
                (target / "_SUCCESS").write_bytes(b"")
                (target / "part-000.snappy.parquet").write_bytes(b"fake-test-payload")
        build(job, package, "run_gha_test_01", "abc123")
        self.assertGreater(validate(package), 12)
        (package / "silver/air_quality_hourly/part-000.snappy.parquet").write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "Invalid file"):
            validate(package)


if __name__ == "__main__":
    unittest.main()
