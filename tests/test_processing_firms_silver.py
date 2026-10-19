
"""Integration tests for FIRMS Silver Parquet."""

import csv
import hashlib
import json
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from src.processing.firms import verified_firms_files


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data/silver/fire_hotspots"


class TestFirmsSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Doc danh sach CSV da duoc manifest xac minh
        paths, _ = verified_firms_files(ROOT, "data")

        cls.raw_records = []

        for path in paths:
            parts = Path(path).parts
            product = parts[parts.index("nasa_firms") + 1]

            with open(path, newline="", encoding="utf-8-sig") as f:
                for record in csv.DictReader(f):
                    cleaned = {
                        k: (v.strip() if v is not None else "")
                        for k, v in record.items()
                    }
                    cls.raw_records.append((product, cleaned))

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Test-FIRMS-Silver")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.df = cls.spark.read.parquet(str(OUTPUT))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def expected_id(product, row):
        # Tinh ID doc lap theo quy tac hotspot-key-v1
        dt = datetime.strptime(
            row["acq_date"] + " " + row["acq_time"].zfill(4),
            "%Y-%m-%d %H%M",
        ).replace(tzinfo=timezone.utc)

        parts = [
            product,
            row["satellite"],
            dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            f'{float(row["latitude"]):.5f}',
            f'{float(row["longitude"]):.5f}',
            row["version"],
            row["bright_ti4"],
            row["scan"],
            row["track"],
            row["frp"],
        ]

        hotspot_id = hashlib.sha256(
            "|".join(parts).encode("utf-8")
        ).hexdigest()

        return hotspot_id, dt

    def test_schema(self):
        # Doi chieu truc tiep voi Data Contract
        contract = json.loads(
            (ROOT / "contracts/schemas/table_contracts.json")
            .read_text(encoding="utf-8")
        )

        fields = contract["tables"]["fire_hotspots"]["fields"]

        self.assertEqual(self.df.columns, list(fields))

        for name, field_type in fields.items():
            expected_type = field_type.rstrip("?")
            if expected_type == "utc":
                expected_type = "timestamp"

            actual_type = (
                self.df.schema[name].dataType.simpleString()
            )

            self.assertEqual(actual_type, expected_type)

    def test_rows_and_unique_ids(self):
        # Dem ID mong doi tu raw, khong hardcode 149
        expected_ids = {
            self.expected_id(product, row)[0]
            for product, row in self.raw_records
        }

        self.assertEqual(self.df.count(), len(expected_ids))

        self.assertEqual(
            self.df.select("hotspot_id").distinct().count(),
            self.df.count(),
        )

    def test_confidence_preserved(self):
        # So sanh phan bo confidence giua Raw va Silver
        expected = Counter(
            row["confidence"] or None
            for _, row in self.raw_records
        )

        actual = {
            row["confidence_raw"]: row["count"]
            for row in (
                self.df.groupBy("confidence_raw")
                .count()
                .collect()
            )
        }

        self.assertEqual(actual, dict(expected))

    def test_hotspot_id_and_time(self):
        # Lay mot quan sat Raw de doi chieu voi Silver
        product, raw = self.raw_records[0]
        expected_id, expected_time = self.expected_id(
            product, raw
        )

        match = (
            self.df
            .filter(F.col("hotspot_id") == expected_id)
            .select(
                F.date_format(
                    "timestamp_utc", "yyyy-MM-dd HH:mm"
                ).alias("time")
            )
            .first()
        )

        self.assertIsNotNone(match)
        self.assertEqual(
            match["time"],
            expected_time.strftime("%Y-%m-%d %H:%M"),
        )


if __name__ == "__main__":
    unittest.main()
