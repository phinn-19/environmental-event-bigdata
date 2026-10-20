"""Kiem thu Curated air_quality_baseline (tich hop + bien thoi gian).

Chay bang Docker tren repo GOC co:
  data/silver/air_quality_hourly/
  data/curated/air_quality_baseline/

Test KHONG ghi de bang Curated that. Bo du lieu gia dinh trong /tmp
chi de kiem tra thuat toan Spark; khong phai du lieu nghien cuu.
"""

import json
import math
import shutil
import statistics
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField,
    StructType, TimestampType,
)

from src.processing.air_quality_baseline import build_baseline


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_FILE = ROOT / "contracts/schemas/table_contracts.json"
CONFIG_FILE = ROOT / "configs/project.yaml"


class TestAirQualityBaseline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))
        cls.rules = cls.config["algorithms"]["air_quality"]["baseline"]
        data_root = cls.config["project"]["data_root"]

        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Test-Air-Quality-Baseline")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        # Doc hai bang THAT da duoc pipeline tao; khong ghi du lieu.
        cls.silver = cls.spark.read.parquet(
            (ROOT / data_root / "silver/air_quality_hourly").as_posix()
        )
        cls.baseline = cls.spark.read.parquet(
            (ROOT / data_root / "curated/air_quality_baseline").as_posix()
        )
        cls.raw_rows = cls.silver.collect()
        cls.result_rows = cls.baseline.collect()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def key(row):
        # Khoa quan trac va baseline phai khop nhau 1-1.
        return (row.sensor_id, row.parameter, row.timestamp_utc)

    def test_schema_contract(self):
        """Ten va kieu cua 7 cot phai khop Data Contract."""
        contract = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))
        fields = contract["tables"]["air_quality_baseline"]["fields"]

        self.assertEqual(self.baseline.columns, list(fields))
        for name, declared_type in fields.items():
            expected_type = declared_type.removesuffix("?")
            if expected_type == "utc":
                expected_type = "timestamp"
            self.assertEqual(
                self.baseline.schema[name].dataType.simpleString(),
                expected_type,
                f"Sai kieu cot {name}",
            )

    def test_one_row_per_silver_observation(self):
        """Ke ca Silver null/flagged van phai co hang baseline tuong ung."""
        input_keys = [self.key(r) for r in self.raw_rows]
        output_keys = [self.key(r) for r in self.result_rows]

        self.assertEqual(len(input_keys), len(output_keys))
        self.assertCountEqual(input_keys, output_keys)

    def test_unique_primary_key(self):
        """Moi (sensor, parameter, timestamp) xuat hien dung mot lan."""
        keys = [self.key(r) for r in self.result_rows]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertTrue(all(all(v is not None for v in k) for k in keys))

    def test_python_reference_median_and_qc(self):
        """Tinh lai BANG PYTHON, doc lap voi Window/median cua Spark.

        - Chi lay 30 ngay TRUOC thoi diem t, khong lay t va tuong lai.
        - Chi lay cung sensor, pollutant va cung gio UTC.
        - Chi dem value huu han, >= 0 va khong flagged (neu config yeu cau).
        - Trung vi phai dung; neu < min_valid_samples thi baseline = NULL.
        """
        lookback = timedelta(days=self.rules["lookback_days"])
        minimum = self.rules["min_valid_samples"]
        exclude_flagged = self.rules["exclude_flagged"]

        history_by_group = defaultdict(list)
        for r in self.raw_rows:
            value = r.value_ugm3
            # math.isfinite bat duoc NaN va infinity; None bi loai.
            if value is None or not math.isfinite(value) or value < 0:
                continue
            if exclude_flagged and r.has_flags is not False:
                continue
            group = (r.sensor_id, r.parameter, r.timestamp_utc.hour)
            history_by_group[group].append((r.timestamp_utc, value))

        for result in self.result_rows:
            t = result.timestamp_utc
            group = (result.sensor_id, result.parameter, t.hour)
            # Dieu kien < t la ranh gioi quan trong nhat: CHONG LEAKAGE.
            values = [
                value for ts, value in history_by_group[group]
                if t - lookback <= ts < t
            ]
            expected_count = len(values)
            expected_usable = expected_count >= minimum

            self.assertEqual(result.sample_count, expected_count, self.key(result))
            self.assertEqual(result.is_usable, expected_usable, self.key(result))
            self.assertEqual(result.method, self.rules["method"])

            if expected_usable:
                self.assertIsNotNone(result.baseline_ugm3)
                self.assertAlmostEqual(
                    result.baseline_ugm3, float(statistics.median(values)),
                    places=7, msg=str(self.key(result)),
                )
            else:
                self.assertIsNone(result.baseline_ugm3, self.key(result))

    def test_synthetic_30_day_boundary_and_leakage(self):
        """Test rieng cac bien ma du lieu that co the chua bao phu.

        Dung workspace TAM trong /tmp, khong dong cham Parquet da nghiem thu.
        10 mau hop le trong 30 ngay => median 55, ke ca mau o dung -30 ngay.
        Mua 'sample' o ngay -31, flagged/null/negative/infinity, khac gio,
        khac sensor, khac parameter va thoi diem hien tai/tuong lai
        KHONG duoc dong gop vao ket qua.
        """
        target = datetime(2024, 2, 1, 9, 0, 0)
        rows = []

        # 10 mau dung cung gio: 10, 20, ..., 100 => median = (50+60)/2 = 55.
        for i in range(10):
            rows.append(("S1", "pm25", target - timedelta(days=30 - i),
                         float((i + 1) * 10), False))

        # Mau ngoai cua so (qua 30 ngay) va cac mau khong dat QC.
        rows.extend([
            ("S1", "pm25", target - timedelta(days=31), 999.0, False),
            ("S1", "pm25", target - timedelta(days=1), 888.0, True),
            ("S1", "pm25", target - timedelta(days=2), None, False),
            ("S1", "pm25", target - timedelta(days=3), -5.0, False),
            ("S1", "pm25", target - timedelta(days=4), float("inf"), False),
            # Khac gio / sensor / pollutant: khong duoc tinh vao S1-pm25-09h.
            ("S1", "pm25", target - timedelta(days=5, hours=-1), 777.0, False),
            ("S2", "pm25", target - timedelta(days=6), 666.0, False),
            ("S1", "pm10", target - timedelta(days=7), 555.0, False),
            # Gia tri dang xet va tuong lai phai bi loai khoi baseline tai t.
            ("S1", "pm25", target, 9999.0, False),
            ("S1", "pm25", target + timedelta(days=1), 10000.0, False),
        ])

        schema = StructType([
            StructField("sensor_id", StringType(), False),
            StructField("parameter", StringType(), False),
            StructField("timestamp_utc", TimestampType(), False),
            StructField("value_ugm3", DoubleType(), True),
            StructField("has_flags", BooleanType(), True),
        ])

        with tempfile.TemporaryDirectory(prefix="baseline_test_") as temp:
            work = Path(temp)
            silver_dir = work / "data/silver/air_quality_hourly"
            silver_dir.parent.mkdir(parents=True)
            (work / "data/curated").mkdir(parents=True)
            (work / "contracts/schemas").mkdir(parents=True)
            shutil.copy2(
                CONTRACT_FILE,
                work / "contracts/schemas/table_contracts.json",
            )

            self.spark.createDataFrame(rows, schema=schema).write.parquet(
                silver_dir.as_posix()
            )
            # Dung config that, chi doi root output sang workspace TAM.
            stats = build_baseline(self.spark, work, self.config)
            result = self.spark.read.parquet(stats["output"])

            target_row = result.filter(
                (result.sensor_id == "S1")
                & (result.parameter == "pm25")
                & (result.timestamp_utc == target)
            ).first()

            self.assertIsNotNone(target_row)
            self.assertEqual(target_row.sample_count, 10)
            self.assertTrue(target_row.is_usable)
            self.assertAlmostEqual(target_row.baseline_ugm3, 55.0, places=7)

            # Mau dau tien chua co du lich su: van co dong, baseline null.
            first = result.filter(
                (result.sensor_id == "S1")
                & (result.parameter == "pm25")
                & (result.timestamp_utc == target - timedelta(days=30))
            ).first()
            self.assertIsNotNone(first)
            self.assertFalse(first.is_usable)
            self.assertIsNone(first.baseline_ugm3)


if __name__ == "__main__":
    unittest.main()
