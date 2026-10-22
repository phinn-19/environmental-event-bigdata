
"""Convert verified OpenAQ raw data to Silver Parquet."""

import argparse
import json
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T

from .openaq_manifest import (
    verify_manifests,
    extract_sensor_metadata,
)


# 1. Schema dau ra: dung 10 cot theo Data Contract
SILVER_SCHEMA = T.StructType([
    T.StructField("sensor_id", T.StringType(), False),
    T.StructField("location_id", T.StringType(), False),
    T.StructField("timestamp_utc", T.TimestampType(), False),
    T.StructField("period_end_utc", T.TimestampType(), False),
    T.StructField("latitude", T.DoubleType(), False),
    T.StructField("longitude", T.DoubleType(), False),
    T.StructField("parameter", T.StringType(), False),
    T.StructField("value_ugm3", T.DoubleType(), True),
    T.StructField("has_flags", T.BooleanType(), True),
    T.StructField("source", T.StringType(), False),
])

# Metadata tu OpenAQ locations
LOCATION_SCHEMA = T.StructType([
    T.StructField("sensor_id", T.StringType(), False),
    T.StructField("location_id", T.StringType(), False),
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("longitude", T.DoubleType(), True),
    T.StructField("parameter", T.StringType(), False),
])

# Cau truc JSON can doc tu OpenAQ
RAW_SCHEMA = T.StructType([
    T.StructField("results", T.ArrayType(T.StructType([
        T.StructField("value", T.DoubleType(), True),
        T.StructField("parameter", T.StructType([
            T.StructField("name", T.StringType(), True),
            T.StructField("units", T.StringType(), True),
        ]), True),
        T.StructField("flagInfo", T.StructType([
            T.StructField("hasFlags", T.BooleanType(), True),
        ]), True),
        T.StructField("period", T.StructType([
            T.StructField("datetimeFrom", T.StructType([
                T.StructField("utc", T.StringType(), True),
            ]), True),
            T.StructField("datetimeTo", T.StructType([
                T.StructField("utc", T.StringType(), True),
            ]), True),
        ]), True),
    ]), True), True),
])

KEY_COLUMNS = ["sensor_id", "parameter", "timestamp_utc"]


def build_silver(spark, repo_root, data_root="data"):
    """Read verified OpenAQ data and create Silver."""

    # 2. Chi doc nhung file da qua kiem tra manifest
    hours, locations = verify_manifests(
        repo_root, data_root
    )

    
    # Kiem tra metadata co bi xung dot khong
    extract_sensor_metadata(locations)

    # Spark doc truc tiep JSON metadata
    location_df = (
        spark.read
        .option("multiLine", True)
        .json([item.path.as_posix() for item in locations])
        .select(F.explode("results").alias("loc"))
        .select(
            F.col("loc.id").cast("string").alias("location_id"),
            F.col("loc.coordinates.latitude").cast("double").alias("latitude"),
            F.col("loc.coordinates.longitude").cast("double").alias("longitude"),
            F.explode("loc.sensors").alias("sensor"),
        )
        .select(
            F.col("sensor.id").cast("string").alias("sensor_id"),
            "location_id",
            "latitude",
            "longitude",
            F.col("sensor.parameter.name").alias("parameter"),
        )
        .dropDuplicates()
    )


    # 3. Doc JSON bang Spark
    paths = [item.path.resolve().as_posix() for item in hours]

    raw = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("multiLine", "true")
        .json(paths)
    )

    # Moi phan tu trong results thanh mot dong
    observations = (
        raw.select(
            F.input_file_name().alias("raw_file"),
            F.explode("results").alias("r")
        )
        .select(
            F.regexp_extract(
                "raw_file", r"sensor=(\d+)", 1
            ).alias("sensor_id"),
            F.col("r.parameter.name").alias("parameter"),
            F.col("r.parameter.units").alias("unit_raw"),
            F.col("r.value").alias("value_ugm3"),
            F.col("r.flagInfo.hasFlags").alias("has_flags"),
            F.to_timestamp(
                "r.period.datetimeFrom.utc"
            ).alias("timestamp_utc"),
            F.to_timestamp(
                "r.period.datetimeTo.utc"
            ).alias("period_end_utc"),
            "raw_file",
        )
    )

    # 4. Ghep measurement voi metadata sensor
    joined = observations.join(
        location_df,
        on=["sensor_id", "parameter"],
        how="left",
    )

    # 5. Kiem tra du lieu
    bad_sensor = (
        F.col("sensor_id").isNull()
        | (F.col("sensor_id") == "")
    )

    bad_parameter = (
        F.col("parameter").isNull()
        | ~F.col("parameter").isin("pm25", "pm10")
    )

    bad_time = (
        F.col("timestamp_utc").isNull()
        | F.col("period_end_utc").isNull()
        | (
            F.col("period_end_utc")
            <= F.col("timestamp_utc")
        )
    )

    bad_location = (
        F.col("location_id").isNull()
        | ~F.col("latitude").between(-90, 90)
        | ~F.col("longitude").between(-180, 180)
        | F.col("latitude").isNull()
        | F.col("longitude").isNull()
    )

    bad_unit = ~F.col("unit_raw").eqNullSafe("µg/m³")

    labelled = joined.withColumn(
        "quarantine_reason",
        F.concat_ws(
            ";",
            F.when(bad_sensor, "sensor_invalid"),
            F.when(bad_parameter, "parameter_invalid"),
            F.when(bad_time, "time_invalid"),
            F.when(bad_location, "location_invalid"),
            F.when(bad_unit, "unit_invalid"),
        )
    )

    rejected = labelled.filter(
        F.length("quarantine_reason") > 0
    )

    valid = labelled.filter(
        F.length("quarantine_reason") == 0
    )

    # 6. Chon dung 10 cot Silver
    silver = valid.select(
        F.col("sensor_id").cast("string"),
        F.col("location_id").cast("string"),
        F.col("timestamp_utc").cast("timestamp"),
        F.col("period_end_utc").cast("timestamp"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("parameter").cast("string"),
        F.col("value_ugm3").cast("double"),
        F.col("has_flags").cast("boolean"),
        F.lit("openaq").alias("source"),
    )

    # 7. Xu ly duplicate
    unique_rows = silver.dropDuplicates(silver.columns)

    conflicts = (
        unique_rows
        .groupBy(*KEY_COLUMNS)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if conflicts:
        raise ValueError(
            f"Co {conflicts} khoa bi trung nhung gia tri khac nhau"
        )

    duplicates_removed = silver.count() - unique_rows.count()

    # Doi chieu ten cot va kieu voi contract
    contract_path = (
        repo_root / "contracts/schemas/table_contracts.json"
    )

    with contract_path.open(encoding="utf-8") as file:
        contract = json.load(file)

    expected = contract["tables"]["air_quality_hourly"]["fields"]

    if unique_rows.columns != list(expected):
        raise ValueError("Silver columns do not match Data Contract")

    for field in SILVER_SCHEMA:
        if unique_rows.schema[field.name].dataType != field.dataType:
            raise ValueError(f"Wrong data type: {field.name}")

    # 8. Thong ke ket qua
    stats = {
        "input_rows": sum(item.count for item in hours),
        "quarantine_rows": rejected.count(),
        "duplicates_removed": duplicates_removed,
        "silver_rows": unique_rows.count(),
        "null_values": unique_rows.filter(
            F.col("value_ugm3").isNull()
        ).count(),
        "flagged_rows": unique_rows.filter(
            F.col("has_flags") == True
        ).count(),
    }

    # 9. Ghi report loi, neu co
    if stats["quarantine_rows"]:
        reject_path = (
            repo_root / data_root
            / "reports/processing/openaq_quarantine"
        )

        rejected.select(
            "raw_file",
            "sensor_id",
            "parameter",
            "quarantine_reason",
        ).write.mode("errorifexists").json(str(reject_path))

    # 10. Luu Silver Parquet
    output = (
        repo_root / data_root / "silver/air_quality_hourly"
    )

    # Empty input van phai xuat dung schema
    to_write = unique_rows

    to_write.write.mode("errorifexists").parquet(str(output))

    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", default="configs/project.yaml"
    )
    args = parser.parse_args()

    root = Path.cwd()

    with (root / args.config).open(encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if (
        config["project"]["timezone"] != "UTC"
        or config["processing"]["timezone"] != "UTC"
    ):
        raise ValueError("Data Contract requires UTC")

    data_root = config["project"]["data_root"]

    if Path(data_root).is_absolute() or ".." in Path(data_root).parts:
        raise ValueError("data_root must be repo-relative")

    # Khoi dong Spark theo cau hinh cua nhom
    spark = (
        SparkSession.builder
        .appName("OpenAQ-Silver")
        .master(config["processing"]["spark_master"])
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.driver.memory",
            config["processing"]["spark_driver_memory"]
        )
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        stats = build_silver(spark, root, data_root)

        for name, value in stats.items():
            print(f"{name}: {value}")

        print("OpenAQ Silver completed")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
