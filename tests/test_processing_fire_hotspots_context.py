"""Tests for Curated fire_hotspots_context (FIRMS + ERA5).

Run after building the Curated table, in Docker env-processing:era5.
- First five tests verify actual output versus an independent Python oracle.
- The last test uses synthetic fixtures under /tmp only: midnight, missing
  rain, matching at minute precision, and FIRMS outside ERA5 HANOI coverage.
NEVER edits/overwrites actual Raw, Silver or Curated project data.
"""

import json
import math
import shutil
import tempfile
import unittest
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from src.processing.fire_hotspots_context import build_context, FIRE_COLS, WX_COLS, CONTEXT_COLS

ROOT = Path(__file__).resolve().parents[1]


def distance_km(lat1, lon1, lat2, lon2):
    """Calculate reference Haversine in native Python (not Spark)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * (
        math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * 6371.0088 * math.asin(math.sqrt(min(1.0, max(0.0, h))))


def expected_nearest(fire, weather_rows):
    """Independent reference: nearest time first, nearest grid second."""
    t = fire["timestamp_utc"]
    start = t.replace(minute=0, second=0, microsecond=0)
    options = []
    for wx in weather_rows:
        t_wx = wx["timestamp_utc"]
        if t_wx not in (start, start + timedelta(hours=1)):
            continue
        offset = abs((t_wx - t).total_seconds())
        if offset > 1800:
            continue
        parts = wx["grid_id"].split(":")
        if len(parts) != 6 or parts[:2] != ["era5", "regular_ll"]:
            raise AssertionError(wx["grid_id"])
        dy, dx = float(parts[2]), float(parts[3])
        lon_delta = ((fire["longitude"] - wx["longitude"] + 180) % 360) - 180
        if abs(fire["latitude"] - wx["latitude"]) > dy / 2 + 1e-8:
            continue
        if abs(lon_delta) > dx / 2 + 1e-8:
            continue
        d = distance_km(
            fire["latitude"], fire["longitude"], wx["latitude"], wx["longitude"]
        )
        options.append((offset, d, t_wx, wx["grid_id"], wx))
    return min(options, key=lambda x: x[:4]) if options else None


def rain_validity(t):
    """FIRMS is instantaneous; rainfall is for its containing UTC hour."""
    return t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


class TestFireHotspotsContext(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.safe_load((ROOT / "configs/project.yaml").read_text(encoding="utf-8"))
        data_root = ROOT / cls.config["project"]["data_root"]
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Test-Fire-Hotspots-Context")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.fire_df = cls.spark.read.parquet((data_root / "silver/fire_hotspots").as_posix())
        cls.wx_df = cls.spark.read.parquet((data_root / "silver/weather_hourly").as_posix())
        cls.out_df = cls.spark.read.parquet((data_root / "curated/fire_hotspots_context").as_posix())
        cls.fire = {r["hotspot_id"]: r.asDict() for r in cls.fire_df.collect()}
        cls.wx = [r.asDict() for r in cls.wx_df.collect()]
        cls.out = {r["hotspot_id"]: r.asDict() for r in cls.out_df.collect()}
        cls.wx_by_key = {(r["grid_id"], r["timestamp_utc"]): r for r in cls.wx}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schema_contract(self):
        """The 18 fields, field order and Spark types match repo contract."""
        fields = json.loads(
            (ROOT / "contracts/schemas/table_contracts.json").read_text(encoding="utf-8")
        )["tables"]["fire_hotspots_context"]["fields"]
        self.assertEqual(len(fields), 18)
        self.assertEqual(self.out_df.columns, list(fields))
        for col, declared in fields.items():
            expected = declared.removesuffix("?").replace("utc", "timestamp")
            self.assertEqual(self.out_df.schema[col].dataType.simpleString(), expected)

    def test_left_join_preserves_every_hotspot(self):
        """Neither a missing ERA5 hour nor distant location may remove a fire row."""
        self.assertEqual(self.fire_df.count(), self.out_df.count())
        self.assertEqual(set(self.fire), set(self.out))
        self.assertEqual(len(self.fire), self.fire_df.count())
        self.assertEqual(len(self.out), self.out_df.count())
        for hotspot_id, row in self.fire.items():
            for col in FIRE_COLS:
                self.assertEqual(self.out[hotspot_id][col], row[col], (hotspot_id, col))

    def test_nearest_grid_and_nearest_validity_hour(self):
        """Reference time/distance/grid selection is calculated in pure Python."""
        for hotspot_id, fire in self.fire.items():
            got = self.out[hotspot_id]
            expected = expected_nearest(fire, self.wx)
            if expected is None:
                for col in CONTEXT_COLS:
                    self.assertIsNone(got[col], (hotspot_id, col))
                continue
            offset, distance, t_wx, grid_id, wx = expected
            self.assertLessEqual(offset, 1800)
            self.assertEqual(got["weather_grid_id"], grid_id, hotspot_id)
            self.assertEqual(got["weather_timestamp_utc"], t_wx, hotspot_id)
            self.assertAlmostEqual(got["weather_distance_km"], distance, places=5)
            for col in ("temperature_c", "wind_u_ms", "wind_v_ms"):
                self.assertEqual(got[col], wx[col], (hotspot_id, col))

    def test_rain_is_same_grid_and_containing_hour(self):
        """Rain for 09:35 is [09:00,10:00), validity 10:00; not rain at 09:00."""
        for hotspot_id, fire in self.fire.items():
            got = self.out[hotspot_id]
            grid_id = got["weather_grid_id"]
            rainfall_source = (
                self.wx_by_key.get((grid_id, rain_validity(fire["timestamp_utc"])))
                if grid_id is not None else None
            )
            want = rainfall_source["precipitation_mm_1h"] if rainfall_source else None
            self.assertEqual(got["precipitation_mm_1h"], want, hotspot_id)

    def test_missing_weather_null_not_zero(self):
        """Outside HANOI weather coverage -> 7 null context cols, no fake 0 km."""
        missing = [r for r in self.out.values() if r["weather_grid_id"] is None]
        for row in missing:
            for col in CONTEXT_COLS:
                self.assertIsNone(row[col], (row["hotspot_id"], col))
        # The actual 1-day VN_BBOX fixture is expected to have missing wx;
        # this also protects against accidentally assigning HANOI to all VN.
        self.assertGreater(len(missing), 0)

    def test_synthetic_minute_midnight_outside_and_null_rain(self):
        """Small independent fixtures: temporal ties, 0 rain, missing rain, outside."""
        from datetime import datetime

        # All synthetic paths are temporary and outside the real project.
        with tempfile.TemporaryDirectory(prefix="fire-context-test-") as tmp:
            base = Path(tmp)
            (base / "contracts/schemas").mkdir(parents=True)
            shutil.copy2(
                ROOT / "contracts/schemas/table_contracts.json",
                base / "contracts/schemas/table_contracts.json",
            )
            (base / "data/silver/fire_hotspots").mkdir(parents=True)
            (base / "data/silver/weather_hourly").mkdir(parents=True)
            cfg = dict(self.config)
            cfg["project"] = dict(cfg["project"], data_root="data")

            # FIRMS observations at arbitrary minutes and spatial positions.
            cases = [
                ("A5", "2024-11-01 09:05", 21.0, 105.75),
                ("B50", "2024-11-01 09:50", 21.0, 106.00),
                ("TIE30", "2024-11-01 09:30", 21.0, 105.75),
                ("OUT", "2024-11-01 09:15", 10.0, 110.0),
                ("MIDNIGHT", "2024-11-01 23:55", 21.0, 105.75),
                ("EXACT", "2024-11-01 09:00", 21.0, 105.75),
                ("NO_RAIN", "2024-11-01 10:10", 21.0, 105.75),
            ]
            fire_rows = [(
                name, datetime.strptime(time, "%Y-%m-%d %H:%M"), lat, lon,
                None, "n", "NOAA-20", "VIIRS", "D", "VIIRS_NOAA20_SP", "nasa_firms",
            ) for name, time, lat, lon in cases]
            fire_schema = StructType([
                StructField("hotspot_id", StringType(), False),
                StructField("timestamp_utc", TimestampType(), False),
                StructField("latitude", DoubleType(), False),
                StructField("longitude", DoubleType(), False),
                StructField("frp_mw", DoubleType(), True),
                StructField("confidence_raw", StringType(), True),
                StructField("satellite", StringType(), False),
                StructField("instrument", StringType(), True),
                StructField("daynight", StringType(), True),
                StructField("source_product", StringType(), False),
                StructField("source", StringType(), False),
            ])

            ga = "era5:regular_ll:0.250000:0.250000:21.000000:105.750000"
            gb = "era5:regular_ll:0.250000:0.250000:21.000000:106.000000"
            def wx_row(grid, time, lon, temp, rain):
                return (grid, datetime.strptime(time, "%Y-%m-%d %H:%M"),
                        21.0, lon, temp, temp - 3, 1.0, -1.0, rain, "era5")
            weather_rows = [
                wx_row(ga, "2024-11-01 09:00", 105.75, 20.0, 99.0),
                wx_row(ga, "2024-11-01 10:00", 105.75, 21.0, 0.7),
                wx_row(gb, "2024-11-01 09:00", 106.00, 22.0, 98.0),
                wx_row(gb, "2024-11-01 10:00", 106.00, 23.0, 0.0),
                wx_row(ga, "2024-11-02 00:00", 105.75, 14.0, 3.5),
            ]
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
            self.spark.createDataFrame(fire_rows, fire_schema).write.parquet(
                (base / "data/silver/fire_hotspots").as_posix(), mode="overwrite"
            )
            self.spark.createDataFrame(weather_rows, wx_schema).write.parquet(
                (base / "data/silver/weather_hourly").as_posix(), mode="overwrite"
            )

            stats = build_context(self.spark, base, cfg)
            self.assertEqual(stats["fire_hotspots_rows"], len(cases))
            self.assertEqual(stats["context_rows"], len(cases))
            got = {r["hotspot_id"]: r.asDict() for r in self.spark.read.parquet(
                (base / "data/curated/fire_hotspots_context").as_posix()
            ).collect()}
            self.assertEqual(got["A5"]["weather_timestamp_utc"].hour, 9)
            self.assertEqual(got["A5"]["temperature_c"], 20.0)
            self.assertEqual(got["A5"]["precipitation_mm_1h"], 0.7)
            self.assertEqual(got["B50"]["weather_grid_id"], gb)
            self.assertEqual(got["B50"]["weather_timestamp_utc"].hour, 10)
            self.assertEqual(got["B50"]["precipitation_mm_1h"], 0.0)
            self.assertEqual(got["TIE30"]["weather_timestamp_utc"].hour, 9)
            self.assertEqual(got["MIDNIGHT"]["weather_timestamp_utc"].day, 2)
            self.assertEqual(got["MIDNIGHT"]["precipitation_mm_1h"], 3.5)
            self.assertEqual(got["EXACT"]["weather_timestamp_utc"].hour, 9)
            self.assertIsNone(got["NO_RAIN"]["precipitation_mm_1h"])
            for name in CONTEXT_COLS:
                self.assertIsNone(got["OUT"][name], name)


if __name__ == "__main__":
    unittest.main()
