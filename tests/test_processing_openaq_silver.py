
import unittest
from pathlib import Path

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

    def test_counts_for_current_sample(self):
        self.assertEqual(self.df.count(), 744)

        self.assertEqual(
            self.df.filter(F.col("value_ugm3").isNull()).count(),
            70,
        )

        self.assertEqual(
            self.df.filter(F.col("has_flags") == True).count(),
            80,
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
