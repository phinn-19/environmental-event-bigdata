
"""Unit tests for OpenAQ Raw hourly coverage."""

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.ingestion.coverage import inspect_openaq_day


class TestOpenAQCoverage(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.raw_path = Path(self.temp_dir.name) / "raw.json"
        self.day = date(2024, 10, 23)

    def make_records(self, hours=range(24)):
        """Create synthetic hourly observations."""
        return [
            {
                "period": {
                    "datetimeFrom": {
                        "utc": f"2024-10-23T{hour:02d}:00:00Z"
                    }
                },
                "value": 35.0,
                "flagInfo": {"hasFlags": False},
            }
            for hour in hours
        ]

    def inspect(self, records):
        self.raw_path.write_text(
            json.dumps({"results": records}),
            encoding="utf-8",
        )
        return inspect_openaq_day(self.raw_path, self.day)

    def test_complete_day(self):
        stats = self.inspect(self.make_records())

        self.assertEqual(stats["rows"], 24)
        self.assertEqual(stats["unique_hours"], 24)
        self.assertEqual(stats["missing_hours"], [])
        self.assertEqual(stats["duplicate_rows"], 0)
        self.assertEqual(stats["usable"], 24)
        self.assertEqual(sum(stats["usable_by_hour"]), 24)

    def test_null_and_flagged(self):
        records = self.make_records()

        records[0]["value"] = None
        records[0]["flagInfo"]["hasFlags"] = True

        records[1]["value"] = 50.0
        records[1]["flagInfo"]["hasFlags"] = True

        records[2]["value"] = None

        stats = self.inspect(records)

        self.assertEqual(stats["null"], 2)
        self.assertEqual(stats["flagged"], 2)
        self.assertEqual(stats["flagged_with_value"], 1)
        self.assertEqual(stats["usable"], 21)
        self.assertEqual(stats["usable_by_hour"][0], 0)
        self.assertEqual(stats["usable_by_hour"][1], 0)
        self.assertEqual(stats["usable_by_hour"][2], 0)
        self.assertEqual(stats["usable_by_hour"][3], 1)

    def test_missing_hour(self):
        stats = self.inspect(
            self.make_records(range(23))
        )

        self.assertEqual(stats["rows"], 23)
        self.assertEqual(stats["missing_hours"], [23])
        self.assertEqual(stats["duplicate_rows"], 0)

    def test_duplicate_hour(self):
        records = self.make_records(range(23))
        records.append(records[22].copy())

        stats = self.inspect(records)

        self.assertEqual(stats["rows"], 24)
        self.assertEqual(stats["unique_hours"], 23)
        self.assertEqual(stats["duplicate_rows"], 1)
        self.assertEqual(stats["missing_hours"], [23])

    def test_wrong_day(self):
        records = self.make_records()
        records[0]["period"]["datetimeFrom"]["utc"] = (
            "2024-10-24T00:00:00Z"
        )

        with self.assertRaises(ValueError):
            self.inspect(records)

    def test_missing_results(self):
        self.raw_path.write_text(
            json.dumps({"metadata": {}}),
            encoding="utf-8",
        )

        with self.assertRaises(ValueError):
            inspect_openaq_day(self.raw_path, self.day)


if __name__ == "__main__":
    unittest.main()
