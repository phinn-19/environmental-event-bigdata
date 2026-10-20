"""Test Curated air_quality_context_hourly: nearest ERA5 + time alignment.

Chay trong Docker env-processing:era5, tai repo da co:
  data/silver/air_quality_hourly/
  data/silver/weather_hourly/
  data/curated/air_quality_context_hourly/

5 test dau CHI DOC Parquet va kiem tra bang phep tinh Python doc lap.
Test cuoi dung du lieu TINH HUONG trong /tmp de kiem tra ngay giao thoa
23:00 -> 00:00 va tinh huong tram nam ngoai luoi. Khong sua Raw/Silver that.
"""

import json
import math
import shutil
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.processing.air_quality_context_hourly import build_context


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/schemas/table_contracts.json"
CONFIG = ROOT / "configs/project.yaml"
AQ_COLS = [
    "sensor_id", "location_id", "timestamp_utc", "period_end_utc",
    "latitude", "longitude", "parameter", "value_ugm3", "has_flags",
    "source",
]
WEATHER_CONTEXT_COLS = [
    "weather_grid_id", "weather_timestamp_utc", "weather_distance_km",
    "temperature_c", "wind_u_ms", "wind_v_ms", "precipitation_mm_1h",
]


def obs_key(row):
    """Unique key cua mot quan trac OpenAQ."""
    return (row["sensor_id"], row["parameter"], row["timestamp_utc"])


def weather_resolution(grid_id):
    """Doc dlat/dlon tu grid_id v1 (KHONG goi ham xu ly chinh)."""
    parts = grid_id.split(":")
    if len(parts) != 6 or parts[:2] != ["era5", "regular_ll"]:
        raise AssertionError(f"Unsupported grid ID: {grid_id}")
    return float(parts[2]), float(parts[3])


def great_circle_km(lat1, lon1, lat2, lon2):
    """Tu tinh khoang cach Haversine bang math; khong phu thuoc Spark UDF."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlon = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(min(1, max(0, h))))


def eligible_grid(aq_row, wx_row):
    """Kiem tra vung phu theo nua buoc luoi nhu rule bao ve cua v1.

    Chi nhung grid co validity time dung gio AQ moi duoc xet.
    Khoang longitude duoc chuan hoa qua kinh tuyen 180 do.
    """
    if wx_row["timestamp_utc"] != aq_row["timestamp_utc"]:
        return False
    dlat, dlon = weather_resolution(wx_row["grid_id"])
    delta_lon = ((aq_row["longitude"] - wx_row["longitude"] + 180) % 360) - 180
    return (
        abs(aq_row["latitude"] - wx_row["latitude"]) <= dlat / 2 + 1e-8
        and abs(delta_lon) <= dlon / 2 + 1e-8
    )


def expected_nearest(aq_row, weather_by_hour):
    """Tim grid nho nhat theo km, neu hoa thi theo grid_id."""
    candidates = []
    for w in weather_by_hour.get(aq_row["timestamp_utc"], []):
        if eligible_grid(aq_row, w):
            distance = great_circle_km(
                aq_row["latitude"], aq_row["longitude"],
                w["latitude"], w["longitude"],
            )
            candidates.append((distance, w["grid_id"], w))
    if not candidates:
        return None, None
    distance, _, weather = min(candidates, key=lambda item: (item[0], item[1]))
    return weather, distance


class TestAirQualityContextHourly(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        data_root = cls.config["project"]["data_root"]

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Test-Air-Quality-Context-Hourly")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        # Doc 3 bang da tao trong project: 2 Silver, 1 Curated.
        cls.aq_df = cls.spark.read.parquet(
            (ROOT / data_root / "silver/air_quality_hourly").as_posix()
        )
        cls.wx_df = cls.spark.read.parquet(
            (ROOT / data_root / "silver/weather_hourly").as_posix()
        )
        cls.context_df = cls.spark.read.parquet(
            (ROOT / data_root / "curated/air_quality_context_hourly").as_posix()
        )

        # Du lieu hien tai nho (pilot 31 ngay / 7 ngay), collect de co
        # reference Python hoan toan doc lap voi phep JOIN Spark.
        # Khi scale Big Data, can test tren sample dai dien thay vi collect.
        cls.aq_rows = [r.asDict() for r in cls.aq_df.collect()]
        cls.wx_rows = [r.asDict() for r in cls.wx_df.collect()]
        cls.context_rows = [r.asDict() for r in cls.context_df.collect()]

        cls.by_hour = defaultdict(list)
        cls.weather_lookup = {}
        for wx in cls.wx_rows:
            cls.by_hour[wx["timestamp_utc"]].append(wx)
            cls.weather_lookup[(wx["grid_id"], wx["timestamp_utc"])] = wx
        cls.context_by_key = {obs_key(r): r for r in cls.context_rows}

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_schema_contract(self):
        """Ten, thu tu va datatype 17 cot phai dung contract trong repo."""
        fields = json.loads(CONTRACT.read_text(encoding="utf-8"))[
            "tables"
        ]["air_quality_context_hourly"]["fields"]
        self.assertEqual(self.context_df.columns, list(fields))
        self.assertEqual(set(fields), set(AQ_COLS + WEATHER_CONTEXT_COLS))
        for col, declared in fields.items():
            expected = declared.removesuffix("?")
            if expected == "utc":
                expected = "timestamp"
            self.assertEqual(
                self.context_df.schema[col].dataType.simpleString(),
                expected, f"Wrong type for {col}",
            )

    def test_left_join_preserves_every_aq_row(self):
        """Khong lam mat/nhan doi AQ, ke ca value null hoac flagged."""
        aq_keys = [obs_key(row) for row in self.aq_rows]
        ctx_keys = [obs_key(row) for row in self.context_rows]
        self.assertEqual(len(ctx_keys), len(aq_keys))
        self.assertEqual(len(ctx_keys), len(set(ctx_keys)))
        self.assertCountEqual(ctx_keys, aq_keys)
        for aq in self.aq_rows:
            result = self.context_by_key[obs_key(aq)]
            for column in AQ_COLS:
                self.assertEqual(result[column], aq[column], (obs_key(aq), column))

    def test_nearest_grid_and_instant_weather(self):
        """Tinh lai nearest tu Python va so sanh grid, distance, T/gio."""
        for aq in self.aq_rows:
            result = self.context_by_key[obs_key(aq)]
            weather, distance = expected_nearest(aq, self.by_hour)
            if weather is None:
                for column in WEATHER_CONTEXT_COLS:
                    self.assertIsNone(result[column], (obs_key(aq), column))
                continue

            self.assertEqual(result["weather_grid_id"], weather["grid_id"])
            self.assertEqual(result["weather_timestamp_utc"], aq["timestamp_utc"])
            self.assertAlmostEqual(
                result["weather_distance_km"], distance, delta=1e-6,
                msg=str(obs_key(aq)),
            )
            for column in ("temperature_c", "wind_u_ms", "wind_v_ms"):
                # Null la gia tri hop le neu bien thoi tiet bi thieu.
                self.assertEqual(result[column], weather[column], (obs_key(aq), column))

    def test_precipitation_matches_next_validity_hour(self):
        """Mua AQ [t,t+1h) phai lay tai weather validity t+1h, CUNG grid."""
        for aq in self.aq_rows:
            result = self.context_by_key[obs_key(aq)]
            selected, _ = expected_nearest(aq, self.by_hour)
            expected = None
            t = aq["timestamp_utc"]
            if selected is not None and aq["period_end_utc"] == t + timedelta(hours=1):
                next_hour = self.weather_lookup.get(
                    (selected["grid_id"], t + timedelta(hours=1))
                )
                if next_hour is not None:
                    expected = next_hour["precipitation_mm_1h"]
            self.assertEqual(
                result["precipitation_mm_1h"], expected,
                f"Rain mismatch at {obs_key(aq)}",
            )

    def test_missing_weather_is_null_not_zero(self):
        """Khong co ERA5 => 7 cot weather null; khong tu dien 0 mm."""
        # So gio khong co grid hop le duoc tinh TU SILVER (doc lap voi output).
        expected_missing = sum(
            expected_nearest(aq, self.by_hour)[0] is None
            for aq in self.aq_rows
        )
        actual_missing = 0
        for aq in self.aq_rows:
            row = self.context_by_key[obs_key(aq)]
            if row["weather_grid_id"] is None:
                actual_missing += 1
                for col in WEATHER_CONTEXT_COLS:
                    self.assertIsNone(row[col], (obs_key(aq), col))
        self.assertEqual(actual_missing, expected_missing)

    def test_synthetic_two_grids_midnight_and_outside_coverage(self):
        """Case bien: 2 grid, qua 00:00, va cam bien o ngoai luoi.

        Fixture chi duoc tao trong /tmp; KHONG phai du lieu API that.
        Gia tri mua 99mm luc t la 'bay' de bat bug lay sai gio.
        """
        t = datetime(2024, 11, 1, 23, 0)
        end = t + timedelta(hours=1)
        gid_a = "era5:regular_ll:0.250000:0.250000:21.000000:105.750000"
        gid_b = "era5:regular_ll:0.250000:0.250000:21.250000:105.750000"

        aq_schema = StructType([
            StructField("sensor_id", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("timestamp_utc", TimestampType(), False),
            StructField("period_end_utc", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("parameter", StringType(), False),
            StructField("value_ugm3", DoubleType(), True),
            StructField("has_flags", BooleanType(), True),
            StructField("source", StringType(), False),
        ])
        wx_schema = StructType([
            StructField("grid_id", StringType(), False),
            StructField("timestamp_utc", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("temperature_c", DoubleType(), True),
            StructField("dewpoint_c", DoubleType(), True),
            StructField("wind_u_ms", DoubleType(), True),
            StructField("wind_v_ms", DoubleType(), True),
            StructField("precipitation_mm_1h", DoubleType(), True),
            StructField("source", StringType(), False),
        ])
        aq_rows = [
            ("A", "L1", t, end, 21.01, 105.76, "pm25", 50.0, False, "openaq"),
            ("B", "L2", t, end, 21.24, 105.74, "pm25", None, True, "openaq"),
            ("OUT", "L3", t, end, 25.0, 110.0, "pm25", 20.0, False, "openaq"),
        ]
        wx_rows = [
            (gid_a, t, 21.0, 105.75, 25.0, 22.0, 1.0, -1.0, 99.0, "era5"),
            (gid_b, t, 21.25, 105.75, 28.0, 24.0, 2.0, -2.0, 88.0, "era5"),
            (gid_a, end, 21.0, 105.75, 26.0, 23.0, 1.2, -1.2, 3.0, "era5"),
            (gid_b, end, 21.25, 105.75, 29.0, 25.0, 2.2, -2.2, 7.0, "era5"),
        ]

        with tempfile.TemporaryDirectory(prefix="aq_context_test_") as temp:
            test_root = Path(temp)
            silver = test_root / "data/silver"
            silver.mkdir(parents=True)
            contract_dir = test_root / "contracts/schemas"
            contract_dir.mkdir(parents=True)
            shutil.copy2(CONTRACT, contract_dir / "table_contracts.json")

            self.spark.createDataFrame(aq_rows, aq_schema).write.parquet(
                (silver / "air_quality_hourly").as_posix()
            )
            self.spark.createDataFrame(wx_rows, wx_schema).write.parquet(
                (silver / "weather_hourly").as_posix()
            )
            stats = build_context(self.spark, test_root, self.config)
            self.assertEqual(stats["air_quality_rows"], 3)
            self.assertEqual(stats["context_rows"], 3)
            self.assertEqual(stats["weather_matched_rows"], 2)
            self.assertEqual(stats["precipitation_matched_rows"], 2)

            result = {
                r.sensor_id: r.asDict()
                for r in self.spark.read.parquet(stats["output"]).collect()
            }
            self.assertEqual(result["A"]["weather_grid_id"], gid_a)
            self.assertEqual(result["B"]["weather_grid_id"], gid_b)
            self.assertEqual(result["A"]["weather_timestamp_utc"], t)
            self.assertEqual(result["A"]["temperature_c"], 25.0)
            self.assertEqual(result["B"]["temperature_c"], 28.0)
            self.assertEqual(result["A"]["precipitation_mm_1h"], 3.0)
            self.assertEqual(result["B"]["precipitation_mm_1h"], 7.0)
            self.assertEqual(result["B"]["value_ugm3"], None)
            self.assertTrue(result["B"]["has_flags"])
            for col in WEATHER_CONTEXT_COLS:
                self.assertIsNone(result["OUT"][col])


if __name__ == "__main__":
    unittest.main()
