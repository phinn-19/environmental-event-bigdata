
import unittest
from pathlib import Path
import json
from src.processing.openaq_manifest import verify_manifests

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data/silver/air_quality_hourly"


class TestOpenAQSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Test-OpenAQ-Silver")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.df = cls.spark.read.parquet(str(OUTPUT))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schema(self):
        expected = [
            ("sensor_id", "string"),
            ("location_id", "string"),
            ("timestamp_utc", "timestamp"),
            ("period_end_utc", "timestamp"),
            ("latitude", "double"),
            ("longitude", "double"),
            ("parameter", "string"),
            ("value_ugm3", "double"),
            ("has_flags", "boolean"),
            ("source", "string"),
        ]

        actual = [
            (field.name, field.dataType.simpleString())
            for field in self.df.schema.fields
        ]

        self.assertEqual(actual, expected)

    
    def test_counts_against_raw(self):
        # Lay danh sach file raw da xac minh
        hours, _ = verify_manifests(ROOT)

        raw_rows = 0
        raw_nulls = 0
        raw_flags = 0

        # Dem truc tiep tu JSON, khong hardcode
        for item in hours:
            with item.path.open(encoding="utf-8") as f:
                records = json.load(f)["results"]

            raw_rows += len(records)

            for record in records:
                if record.get("value") is None:
                    raw_nulls += 1

                if (
                    (record.get("flagInfo") or {})
                    .get("hasFlags") is True
                ):
                    raw_flags += 1

        silver_rows = self.df.count()

        # Silver khong the nhieu hon Raw khi chi loai loi/trung
        self.assertLessEqual(silver_rows, raw_rows)

        # Neu khong mat dong nao, null va flag phai duoc giu
        if silver_rows == raw_rows:
            self.assertEqual(
                self.df.filter(
                    F.col("value_ugm3").isNull()
                ).count(),
                raw_nulls,
            )

            self.assertEqual(
                self.df.filter(
                    F.col("has_flags") == True
                ).count(),
                raw_flags,
            )

    def test_no_duplicate_keys(self):
        duplicates = (
            self.df
            .groupBy("sensor_id", "parameter", "timestamp_utc")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        self.assertEqual(duplicates, 0)


if __name__ == "__main__":
    unittest.main()
