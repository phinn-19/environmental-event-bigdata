"""Kiem thu ERA5 Silver bang cach doi chieu voi GRIB goc.

Chay trong Docker env-processing:era5 (da cai eccodes va pyspark).
Test CHI DOC Raw/Silver: khong ghi de Parquet, khong doi Data Contract.
"""

import json
import math
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import eccodes
from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "data/raw/era5"
SILVER_DIR = ROOT / "data/silver/weather_hourly"
QUALITY_FILE = SILVER_DIR / "_era5_quality.json"
CONTRACT_FILE = ROOT / "contracts/schemas/table_contracts.json"

# Map ten bien GRIB sang ten cot trong Silver.
VARIABLES = {
    "2t": "temperature_c",
    "2d": "dewpoint_c",
    "10u": "wind_u_ms",
    "10v": "wind_v_ms",
    "tp": "precipitation_mm_1h",
}


def expected_grid_id(lat, lon, dlat, dlon):
    """Tao grid_id on dinh tu toa do + do phan giai luoi ERA5."""
    lat = round(float(lat), 6)
    lon = round(((float(lon) + 180) % 360) - 180, 6)
    return f"era5:regular_ll:{dlat:.6f}:{dlon:.6f}:{lat:.6f}:{lon:.6f}"


def read_raw_reference():
    """Doc GRIB bang ecCodes de lay ket qua ky vong, doc lap voi Spark.

    - expected_keys: so (diem luoi, gio UTC) phai co o Silver.
    - samples: mot gia tri thuc cua moi bien de kiem tra chuyen doi don vi.
    - messages/points: doi chieu voi bao cao chat luong sau Processing.

    Du lieu pilot nho (9 diem x 24 gio). Khi raw rat lon, co the tach
    bo kiem tra toan bo keys thanh bai integration test rieng.
    """
    manifests = sorted((RAW_ROOT / "manifests").rglob("*.manifest.json"))
    if not manifests:
        raise FileNotFoundError("Khong tim thay manifest ERA5")

    keys = set()
    samples = {}
    validity_hours = set()
    variable_messages = Counter()
    messages = 0
    variable_rows = 0

    for manifest_path in manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        grib_path = (ROOT / manifest["file"]).resolve()
        if not grib_path.is_relative_to(RAW_ROOT.resolve()):
            raise ValueError("Duong dan GRIB nam ngoai RAW_ROOT")

        this_file_messages = 0
        with grib_path.open("rb") as stream:
            while True:
                gid = eccodes.codes_grib_new_from_file(stream)
                if gid is None:
                    break
                try:
                    # validityDate/validityTime la gio hieu luc, KHONG phai
                    # ngay/gio bat dau du bao (dataDate/dataTime).
                    var = eccodes.codes_get(gid, "shortName")
                    if var not in VARIABLES:
                        raise ValueError(f"Bien ERA5 khong mong doi: {var}")
                    date_text = f'{int(eccodes.codes_get(gid, "validityDate")):08d}'
                    time_text = f'{int(eccodes.codes_get(gid, "validityTime")):04d}'
                    valid = datetime.strptime(
                        date_text + time_text, "%Y%m%d%H%M"
                    ).replace(tzinfo=timezone.utc)
                    valid_text = valid.strftime("%Y-%m-%d %H:%M:%S")

                    dlat = float(eccodes.codes_get(gid, "jDirectionIncrementInDegrees"))
                    dlon = float(eccodes.codes_get(gid, "iDirectionIncrementInDegrees"))
                    lats = eccodes.codes_get_array(gid, "latitudes")
                    lons = eccodes.codes_get_array(gid, "longitudes")
                    values = eccodes.codes_get_array(gid, "values")
                    missing = float(eccodes.codes_get(gid, "missingValue"))
                    if not (len(lats) == len(lons) == len(values)):
                        raise ValueError("GRIB khong khop so toa do va gia tri")

                    messages += 1
                    this_file_messages += 1
                    variable_messages[var] += 1
                    variable_rows += len(values)
                    validity_hours.add(valid_text)

                    for lat, lon, value in zip(lats, lons, values):
                        grid_id = expected_grid_id(lat, lon, dlat, dlon)
                        keys.add((grid_id, valid_text))

                        # Lay mot mau hop le cho moi bien, khong hardcode gia tri.
                        if var not in samples and math.isfinite(float(value)) and float(value) != missing:
                            value = float(value)
                            # ecCodes luu nhiet do bang K, luong mua bang m.
                            if var in ("2t", "2d"):
                                expected = value - 273.15
                            elif var == "tp":
                                expected = value * 1000.0
                            else:
                                expected = value
                            samples[var] = {
                                "grid_id": grid_id,
                                "time": valid_text,
                                "expected": expected,
                                "step_type": str(eccodes.codes_get(gid, "stepType")),
                                "step_range": str(eccodes.codes_get(gid, "stepRange")),
                            }
                finally:
                    # Bat buoc giai phong message GRIB de khong ro ri RAM.
                    eccodes.codes_release(gid)

        if this_file_messages != manifest["row_count"]:
            raise AssertionError(
                f"GRIB/manifest lech so messages: {grib_path.name}"
            )

    return {
        "keys": keys,
        "samples": samples,
        "validity_hours": validity_hours,
        "messages": messages,
        "variable_rows": variable_rows,
        "variable_messages": dict(variable_messages),
    }


class TestERA5Silver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # GRIB la nguon ky vong; Parquet la ket qua can kiem thu.
        cls.raw = read_raw_reference()
        if not SILVER_DIR.is_dir():
            raise FileNotFoundError(f"Chua tao Silver: {SILVER_DIR}")
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Test-ERA5-Silver")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.df = cls.spark.read.parquet(str(SILVER_DIR))

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()
    # kiểm tra đún 10 cột theo data contract

    def test_schema_contract(self):
        """Bang phai co dung 10 cot va datatype theo Data Contract."""
        contract = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))
        fields = contract["tables"]["weather_hourly"]["fields"]
        self.assertEqual(self.df.columns, list(fields))
        for name, declared in fields.items():
            expected_type = declared.rstrip("?")
            if expected_type == "utc":
                expected_type = "timestamp"
            actual_type = self.df.schema[name].dataType.simpleString()
            self.assertEqual(actual_type, expected_type, name)
    # không trùng điểm lưới, giờ, không thiếu trường bắt buộc
    def test_unique_keys_and_required_values(self):
        """Moi diem luoi/gio co dung 1 dong, cac truong bat buoc khong null."""
        df = self.df
        count = df.count()
        self.assertEqual(
            df.select("grid_id", "timestamp_utc").distinct().count(), count
        )
        bad = (
            F.col("grid_id").isNull()
            | F.col("timestamp_utc").isNull()
            | F.col("latitude").isNull()
            | F.col("longitude").isNull()
            | ~F.col("latitude").between(-90, 90)
            | ~F.col("longitude").between(-180, 180)
            | F.col("source").isNull()
            | (F.col("source") != "era5")
        )
        self.assertEqual(df.filter(bad).limit(1).count(), 0)
    # đối chiếu số dòng và giờ với GRIB gốc
    def test_rows_and_validity_hours_against_raw(self):
        """Tu tinh so diem/gio tu GRIB, khong hardcode 216 dong."""
        self.assertEqual(self.df.count(), len(self.raw["keys"]))
        actual_hours = {
            row["time"] for row in self.df.select(
                F.date_format("timestamp_utc", "yyyy-MM-dd HH:mm:ss").alias("time")
            ).distinct().collect()
        }
        self.assertEqual(actual_hours, self.raw["validity_hours"])
    # kiểm tra kelvin-> c, mưa -> mm, đúng giờ hiệu lực

    def test_values_and_precipitation_time(self):
        """Doi chieu 5 bien: K->C, gio m/s, va tp m->mm o GIO KET THUC."""
        samples = self.raw["samples"]
        self.assertEqual(set(samples), set(VARIABLES))
        for var, sample in samples.items():
            # Tim dung dong theo grid_id va validity UTC tu GRIB.
            matched = (
                self.df.filter(F.col("grid_id") == sample["grid_id"])
                .filter(
                    F.date_format("timestamp_utc", "yyyy-MM-dd HH:mm:ss")
                    == sample["time"]
                )
                .select(VARIABLES[var])
                .collect()
            )
            self.assertEqual(len(matched), 1, f"Thieu/trung dong cho {var}")
            actual = matched[0][0]
            self.assertIsNotNone(actual, f"Bien {var} bi null")
            self.assertAlmostEqual(
                actual, sample["expected"], delta=1e-6,
                msg=f"Sai gia tri/doi don vi cho {var}",
            )
            if var == "tp":
                self.assertEqual(sample["step_type"], "accum")
                start_step, end_step = map(int, sample["step_range"].split("-"))
                self.assertEqual(end_step - start_step, 1)
                # timestamp trong truy van la validity time: gio ket thuc mua.
    # kiểm tra báo cáo khớp với dl thực.
    def test_quality_report(self):
        """Bao cao _era5_quality.json phai khop GRIB va Parquet thuc te."""
        self.assertTrue(QUALITY_FILE.is_file())
        qc = json.loads(QUALITY_FILE.read_text(encoding="utf-8"))
        self.assertEqual(qc["messages"], self.raw["messages"])
        self.assertEqual(qc["raw_variable_rows"], self.raw["variable_rows"])
        self.assertEqual(qc["variable_messages"], self.raw["variable_messages"])
        self.assertEqual(qc["silver_rows"], self.df.count())
        self.assertEqual(
            qc["null_precipitation_rows"],
            self.df.filter(F.col("precipitation_mm_1h").isNull()).count(),
        )


if __name__ == "__main__":
    unittest.main()
