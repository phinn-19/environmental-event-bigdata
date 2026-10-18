
"""Tests for OpenAQ manifest verification."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.processing.openaq_manifest import (
    extract_sensor_metadata,
    verify_manifests,
)


class ManifestContractTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

        self.root = Path(self.temp.name)
        raw = self.root / "data/raw/openaq"

        self.hour = (
            raw / "hours/sensor=21632/year=2024/"
            "month=11/day=01/page=0001.json"
        )
        self.hour.parent.mkdir(parents=True)

        self.loc = raw / "locations/location.json"
        self.loc.parent.mkdir(parents=True)

        hour_data = {
            "results": [{
                "value": None,
                "flagInfo": {"hasFlags": True},
                "parameter": {
                    "name": "pm25",
                    "units": "µg/m³"
                },
                "period": {
                    "datetimeFrom": {
                        "utc": "2024-11-01T09:00:00Z"
                    },
                    "datetimeTo": {
                        "utc": "2024-11-01T10:00:00Z"
                    }
                }
            }]
        }

        self.hour.write_text(
            json.dumps(hour_data),
            encoding="utf-8",
        )

        location_data = {
            "results": [{
                "id": 7441,
                "coordinates": {
                    "latitude": 21.021939,
                    "longitude": 105.818806,
                },
                "sensors": [{
                    "id": 21632,
                    "parameter": {"name": "pm25"}
                }]
            }]
        }

        self.loc.write_text(
            json.dumps(location_data),
            encoding="utf-8",
        )

        items = [
            (
                "openaq_v3_hours",
                self.hour,
                "sensor=21632/year=2024/month=11/"
                "day=01/page=0001.manifest.json",
            ),
            (
                "openaq_v3_locations",
                self.loc,
                "locations/location.manifest.json",
            ),
        ]

        for product, file, relative_manifest in items:
            manifest_path = (
                raw / "manifests" / relative_manifest
            )
            manifest_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            manifest = {
                "schema_version": "manifest-v1",
                "source": "openaq",
                "source_product": product,
                "acquisition_mode": "live",
                "status": "success",
                "file": file.relative_to(
                    self.root
                ).as_posix(),
                "bytes": file.stat().st_size,
                "sha256": hashlib.sha256(
                    file.read_bytes()
                ).hexdigest(),
                "row_count": 1,
            }

            manifest_path.write_text(
                json.dumps(manifest),
                encoding="utf-8",
            )

    def test_verified_files_and_sensor_lookup(self):
        hours, locations = verify_manifests(self.root)

        self.assertEqual(
            sum(p.count for p in hours),
            1,
        )

        self.assertEqual(
            extract_sensor_metadata(locations),
            [(
                "21632",
                "7441",
                21.021939,
                105.818806,
                "pm25",
            )],
        )

    def test_sha_mismatch_is_not_ignored(self):
        self.hour.write_text(
            self.hour.read_text(encoding="utf-8") + " ",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Byte count mismatch|SHA256 mismatch",
        ):
            verify_manifests(self.root)

    def test_manifest_row_count_mismatch_is_not_ignored(self):
        manifest_path = (
            self.root
            / "data/raw/openaq/manifests/"
            "locations/location.manifest.json"
        )

        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
        manifest["row_count"] = 2

        manifest_path.write_text(
            json.dumps(manifest),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            ValueError,
            "row_count mismatch",
        ):
            verify_manifests(self.root)


if __name__ == "__main__":
    unittest.main()
