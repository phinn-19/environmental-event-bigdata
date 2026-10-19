"""Convert verified ERA5 GRIB messages to Silver weather_hourly Parquet."""

import argparse
import csv
import hashlib
import json
import math
import re
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import eccodes
import yaml
from pyspark.sql import SparkSession, functions as F, types as T


VARIABLES = {
    "2t": "temperature_c",
    "2d": "dewpoint_c",
    "10u": "wind_u_ms",
    "10v": "wind_v_ms",
    "tp": "precipitation_mm_1h",
}

WEATHER_SCHEMA = T.StructType([
    T.StructField("grid_id", T.StringType(), False),
    T.StructField("timestamp_utc", T.TimestampType(), False),
    T.StructField("latitude", T.DoubleType(), False),
    T.StructField("longitude", T.DoubleType(), False),
    T.StructField("temperature_c", T.DoubleType(), True),
    T.StructField("dewpoint_c", T.DoubleType(), True),
    T.StructField("wind_u_ms", T.DoubleType(), True),
    T.StructField("wind_v_ms", T.DoubleType(), True),
    T.StructField("precipitation_mm_1h", T.DoubleType(), True),
    T.StructField("source", T.StringType(), False),
])

STAGING_SCHEMA = T.StructType([
    T.StructField("grid_id", T.StringType(), True),
    T.StructField("timestamp_text", T.StringType(), True),
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("longitude", T.DoubleType(), True),
    T.StructField("variable", T.StringType(), True),
    T.StructField("value", T.DoubleType(), True),
])


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_instant(text):
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
        raise ValueError(f"Timestamp is not UTC: {text}")
    return dt


def verified_files(root, data_root):
    raw_root = (root / data_root / "raw/era5").resolve()
    paths = sorted((raw_root / "manifests").rglob("*.manifest.json"))
    if not paths:
        raise FileNotFoundError("No ERA5 manifests found")

    verified = []
    seen = set()
    for mp in paths:
        m = json.loads(mp.read_text(encoding="utf-8"))
        if (m.get("schema_version") != "manifest-v1"
                or m.get("source") != "era5"
                or m.get("source_product") != "reanalysis-era5-single-levels"
                or m.get("format") != "grib"
                or m.get("status") not in ("success", "empty")):
            raise ValueError(f"Invalid ERA5 manifest: {mp}")

        file_name = m.get("file")
        if not isinstance(file_name, str):
            raise ValueError(f"Missing GRIB file path: {mp}")
        path = (root / file_name).resolve()
        expected_parent = raw_root / m["source_product"] / m["region_id"]
        if not path.is_relative_to(expected_parent) or not path.is_file():
            raise ValueError(f"Unsafe/missing GRIB path: {mp}")
        if path.stat().st_size != m.get("bytes") or sha256_file(path) != m.get("sha256"):
            raise ValueError(f"GRIB checksum/size mismatch: {path}")
        start, end = utc_instant(m["start_utc"]), utc_instant(m["end_utc"])
        if start >= end or not isinstance(m.get("row_count"), int) or m["row_count"] < 0:
            raise ValueError(f"Invalid window/message count: {mp}")
        if path in seen:
            raise ValueError(f"GRIB file declared more than once: {path}")
        seen.add(path)
        verified.append((path, start, end, m["row_count"]))
    return verified


def grid_resolution(gid):
    grid_type = eccodes.codes_get(gid, "gridType")
    if grid_type != "regular_ll":
        raise ValueError(f"Unsupported ERA5 grid type: {grid_type}")
    dlat = float(eccodes.codes_get(gid, "jDirectionIncrementInDegrees"))
    dlon = float(eccodes.codes_get(gid, "iDirectionIncrementInDegrees"))
    if not (0 < dlat <= 5 and 0 < dlon <= 5):
        raise ValueError(f"Invalid grid resolution: {dlat}, {dlon}")
    return dlat, dlon


def grid_id_of(lat, lon, dlat, dlon):
    lat = round(float(lat), 6)
    lon = round(((float(lon) + 180) % 360) - 180, 6)
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError(f"Invalid grid coordinate: {lat}, {lon}")
    key = f"era5:regular_ll:{dlat:.6f}:{dlon:.6f}:{lat:.6f}:{lon:.6f}"
    return key, lat, lon


def is_one_hour_precip(gid):
    # Validate accumulation over exactly one hour ending at validity time.
    if eccodes.codes_get(gid, "stepType") != "accum":
        return False
    units = str(eccodes.codes_get(gid, "stepUnits"))
    if units not in ("h", "1", "hour", "hours"):
        return False
    step = str(eccodes.codes_get(gid, "stepRange"))
    if not re.fullmatch(r"\d+-\d+", step):
        return False
    beginning, ending = map(int, step.split("-"))
    return ending - beginning == 1


def clean_value(value, missing, var, rain_ok):
    value = float(value)
    if not math.isfinite(value) or value == missing:
        return None
    if var == "tp":
        if not rain_ok:
            return None
        if value < 0:
            raise ValueError(f"Negative ERA5 precipitation: {value}")
        return value * 1000.0
    if var in ("2t", "2d"):
        if not 150 <= value <= 350:
            raise ValueError(f"Unexpected temperature (K): {value}")
        return value - 273.15
    return value


def decode_to_csv(verified, csv_path):
    messages = 0
    rows = 0
    invalid_precip = []
    names = Counter()

    with csv_path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow([f.name for f in STAGING_SCHEMA.fields])

        for path, start, end, manifest_count in verified:
            file_messages = 0
            with path.open("rb") as f:
                while True:
                    gid = eccodes.codes_grib_new_from_file(f)
                    if gid is None:
                        break
                    try:
                        var = eccodes.codes_get(gid, "shortName")
                        if var not in VARIABLES:
                            raise ValueError(f"Unknown ERA5 variable: {var}")
                        valid = datetime.strptime(
                            f'{int(eccodes.codes_get(gid, "validityDate")):08d} '
                            f'{int(eccodes.codes_get(gid, "validityTime")):04d}',
                            "%Y%m%d %H%M",
                        ).replace(tzinfo=timezone.utc)
                        if not start <= valid < end:
                            raise ValueError(f"GRIB validity outside manifest window: {valid} in {path}")
                        if valid.minute != 0 or valid.second != 0:
                            raise ValueError(f"Non-hourly GRIB validity: {valid}")

                        unit = str(eccodes.codes_get(gid, "units"))
                        expected = ("K",) if var in ("2t", "2d") else (("m", "m of water equivalent") if var == "tp" else ("m s**-1",))
                        if unit not in expected:
                            raise ValueError(f"Wrong ERA5 units: {var}={unit}")

                        rain_ok = True
                        if var == "tp":
                            rain_ok = is_one_hour_precip(gid)
                            if not rain_ok:
                                invalid_precip.append({
                                    "file": path.as_posix(),
                                    "validity": valid.isoformat(),
                                    "reason": "precipitation step is not a verified 1h accumulation",
                                })
                        elif eccodes.codes_get(gid, "stepType") != "instant":
                            raise ValueError(f"Non-instantaneous ERA5 variable: {var}")

                        missing = float(eccodes.codes_get(gid, "missingValue"))
                        lats = eccodes.codes_get_array(gid, "latitudes")
                        lons = eccodes.codes_get_array(gid, "longitudes")
                        vals = eccodes.codes_get_array(gid, "values")
                        if len(lats) != len(lons) or len(lats) != len(vals) or len(vals) != int(eccodes.codes_get(gid, "numberOfPoints")):
                            raise ValueError("GRIB coordinate/value count mismatch")

                        dlat, dlon = grid_resolution(gid)
                        for lat, lon, val in zip(lats, lons, vals):
                            grid_id, lat, lon = grid_id_of(lat, lon, dlat, dlon)
                            mapped = clean_value(val, missing, var, rain_ok)
                            writer.writerow([grid_id, valid.strftime("%Y-%m-%d %H:%M:%S"), lat, lon, var, mapped])
                            rows += 1
                        file_messages += 1
                        messages += 1
                        names[var] += 1
                    finally:
                        eccodes.codes_release(gid)
            if file_messages != manifest_count:
                raise ValueError(f"GRIB message count differs from manifest: {path} ({file_messages} != {manifest_count})")
            print(f"Verified GRIB: {path.name} ({file_messages} messages)")

    return {"messages": messages, "raw_variable_rows": rows, "variable_messages": dict(names), "invalid_precip_messages": invalid_precip}


def build_silver(spark, root, data_root):
    output = root / data_root / "silver/weather_hourly"
    if output.exists():
        raise FileExistsError(f"Silver output already exists: {output}")
    verified = verified_files(root, data_root)

    # Stage decoded observations on disk rather than accumulating all GRIB hours in driver memory.
    with tempfile.TemporaryDirectory(prefix="era5_decode_") as tmp:
        stage_path = Path(tmp) / "observations.csv"
        qc = decode_to_csv(verified, stage_path)
        qc["raw_files"] = [str(path.relative_to(root)) for path, _, _, _ in verified]
        qc["grid_id_rule"] = "era5:regular_ll:dlat:dlon:latitude:longitude (6 decimals)"
        qc["precipitation_rule"] = "1h accumulation ending at validity UTC, metres to mm"

        if qc["raw_variable_rows"] == 0:
            silver = spark.createDataFrame([], WEATHER_SCHEMA)
        else:
            narrow = spark.read.schema(STAGING_SCHEMA).option("header", True).option("mode", "FAILFAST").csv(str(stage_path))
            narrow = narrow.withColumn("timestamp_utc", F.to_timestamp("timestamp_text", "yyyy-MM-dd HH:mm:ss")).drop("timestamp_text")

            if narrow.filter(
                F.col("grid_id").isNull() | F.col("timestamp_utc").isNull()
                | F.col("latitude").isNull() | F.col("longitude").isNull()
                | F.col("variable").isNull()
            ).limit(1).count():
                raise ValueError("Invalid staged weather observation")

            conflicts = (
                narrow.groupBy("grid_id", "timestamp_utc", "variable")
                .agg(F.min("value").alias("lo"), F.max("value").alias("hi"))
                .filter(F.abs(F.col("hi") - F.col("lo")) > F.lit(1e-8))
                .limit(1).count()
            )
            if conflicts:
                raise ValueError("Overlapping ERA5 files disagree on the same grid/hour/variable")

            wide = (
                narrow.groupBy("grid_id", "timestamp_utc", "latitude", "longitude")
                .pivot("variable", list(VARIABLES))
                .agg(F.max("value"))
            )
            silver = wide.select(
                "grid_id", "timestamp_utc", "latitude", "longitude",
                F.col("2t").alias("temperature_c"),
                F.col("2d").alias("dewpoint_c"),
                F.col("10u").alias("wind_u_ms"),
                F.col("10v").alias("wind_v_ms"),
                F.col("tp").alias("precipitation_mm_1h"),
                F.lit("era5").alias("source"),
            )

            if (
                silver.groupBy("grid_id", "timestamp_utc").count()
                .filter(F.col("count") > 1).limit(1).count()
            ):
                raise ValueError("Duplicate ERA5 Silver keys")

        contract = json.loads((root / "contracts/schemas/table_contracts.json").read_text(encoding="utf-8"))
        fields = contract["tables"]["weather_hourly"]["fields"]
        if silver.columns != list(fields):
            raise ValueError("ERA5 Silver columns do not match contract")
        for col_name, declared in fields.items():
            actual = silver.schema[col_name].dataType.simpleString()
            required = declared.rstrip("?").replace("utc", "timestamp")
            if actual != required:
                raise ValueError(f"ERA5 contract type mismatch {col_name}: {actual} != {required}")

        qc["silver_rows"] = silver.count()
        qc["null_precipitation_rows"] = silver.filter(F.col("precipitation_mm_1h").isNull()).count()
        silver.write.mode("errorifexists").parquet(str(output))
        # Spark ignores files beginning with '_' when reading Parquet directory.
        (output / "_era5_quality.json").write_text(json.dumps(qc, indent=2), encoding="utf-8")
        return qc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/project.yaml")
    args = parser.parse_args()
    root = Path.cwd()
    config = yaml.safe_load((root / args.config).read_text(encoding="utf-8"))
    if config["project"]["timezone"] != "UTC" or config["processing"]["timezone"] != "UTC":
        raise ValueError("UTC required by Data Contract")
    data_root = config["project"]["data_root"]
    if Path(data_root).is_absolute() or ".." in Path(data_root).parts:
        raise ValueError("data_root must be relative to project")
    spark = (SparkSession.builder.appName("ERA5-Silver")
        .master(config["processing"]["spark_master"])
        .config("spark.driver.memory", config["processing"]["spark_driver_memory"])
        .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        spark.sparkContext.setLogLevel("ERROR")
        qc = build_silver(spark, root, data_root)
        for k, v in qc.items():
            if k in ("messages", "raw_variable_rows", "variable_messages", "silver_rows", "null_precipitation_rows"):
                print(f"{k}: {v}")
        print("invalid_precip_messages:", len(qc["invalid_precip_messages"]))
        print("ERA5 Silver completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
