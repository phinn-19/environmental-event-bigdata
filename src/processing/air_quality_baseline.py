"""Create the air_quality_baseline Curated table from OpenAQ Silver.

Algorithm v1 (configured in configs/project.yaml):
- Group by sensor, pollutant and UTC hour-of-day.
- Use only valid observations strictly before the target timestamp,
  within the preceding 30 * 24 hours.
- Take the EXACT median if at least min_valid_samples exist.
- Preserve an output row for every input Silver observation, including
  observations whose own measurement is null or flagged.

This module reads Silver Parquet; it never reads or modifies Raw data.
"""

import argparse
import json
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, Window, functions as F


# Day la 7 cot va thu tu theo DATA_CONTRACT_V1.
OUTPUT_COLUMNS = [
    "sensor_id",
    "parameter",
    "timestamp_utc",
    "baseline_ugm3",
    "sample_count",
    "method",
    "is_usable",
]


def validate_contract(df, root: Path) -> None:
    """Check output column names and types against the repo contract."""
    path = root / "contracts/schemas/table_contracts.json"
    contract = json.loads(path.read_text(encoding="utf-8"))
    fields = contract["tables"]["air_quality_baseline"]["fields"]

    if df.columns != list(fields):
        raise ValueError(
            f"Baseline columns differ from contract: {df.columns}"
        )

    for name, declared_type in fields.items():
        # Contract uses 'utc' for Spark timestamp and '?' for nullable.
        expected = declared_type.removesuffix("?")
        if expected == "utc":
            expected = "timestamp"
        actual = df.schema[name].dataType.simpleString()
        if actual != expected:
            raise ValueError(
                f"Baseline type mismatch for {name}: {actual} != {expected}"
            )


def build_baseline(spark, root: Path, config: dict) -> dict:
    """Compute baseline without any future-data leakage or output overwrite."""
    data_root = config["project"]["data_root"]
    if Path(data_root).is_absolute() or ".." in Path(data_root).parts:
        raise ValueError("data_root must be a relative directory")

    if (
        config["project"]["timezone"] != "UTC"
        or config["processing"]["timezone"] != "UTC"
    ):
        raise ValueError("Baseline v1 requires UTC")

    rules = config["algorithms"]["air_quality"]["baseline"]
    method = rules["method"]
    lookback_days = rules["lookback_days"]
    min_samples = rules["min_valid_samples"]
    exclude_flagged = rules["exclude_flagged"]

    # Ten method co '30d'; khong am tham cho phep config 7d nhung van
    # ghi version '30d' (se lam sai nghia ket qua giao cho Nguoi 3).
    if method != "same_utc_hour_median_30d_v1" or lookback_days != 30:
        raise ValueError("Unsupported baseline method/window for v1")
    if type(min_samples) is not int or min_samples < 1:
        raise ValueError("min_valid_samples must be a positive integer")
    if type(exclude_flagged) is not bool:
        raise ValueError("exclude_flagged must be boolean")

    input_path = root / data_root / "silver/air_quality_hourly"
    output_path = root / data_root / "curated/air_quality_baseline"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing OpenAQ Silver: {input_path}")
    if output_path.exists():
        raise FileExistsError(f"Baseline already exists: {output_path}")

    aq = spark.read.parquet(input_path.as_posix())
    required = {
        "sensor_id", "parameter", "timestamp_utc", "value_ugm3", "has_flags"
    }
    if not required.issubset(aq.columns):
        raise ValueError(f"OpenAQ Silver missing columns: {required - set(aq.columns)}")

    # Moi timestamp (sensor, parameter) phai duy nhat; neu raw da trung
    # thi Window se dem sai so mau, nen bao loi thay vi tinh tiep.
    keys = ["sensor_id", "parameter", "timestamp_utc"]
    if aq.filter(
        F.col("sensor_id").isNull()
        | F.col("parameter").isNull()
        | F.col("timestamp_utc").isNull()
    ).limit(1).count():
        raise ValueError("OpenAQ Silver contains null primary keys")
    if (
        aq.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("OpenAQ Silver contains duplicated primary keys")

    # QC: chi gia tri huu han >= 0 moi duoc dong gop vao baseline.
    # Khi exclude_flagged=true, chi has_flags=False moi duoc tinh.
    # has_flags=NULL la chua biet trang thai QC -> bo khoi mau nen.
    value = F.col("value_ugm3")
    valid_value = (
        value.isNotNull()
        & ~F.isnan(value)
        & (value >= F.lit(0.0))
        & (value < F.lit(float("inf")))
    )
    flag_ok = (
        F.col("has_flags") == F.lit(False)
        if exclude_flagged else F.lit(True)
    )
    eligible = valid_value & flag_ok

    prepared = (
        aq.select(*keys, "value_ugm3", "has_flags")
        # Timezone Spark da duoc khoa ve UTC trong main().
        .withColumn("_hour_utc", F.hour("timestamp_utc"))
        .withColumn("_epoch_s", F.col("timestamp_utc").cast("long"))
        # collect_list bo qua NULL: mau khong dat QC khong tham gia median.
        .withColumn("_valid_sample", F.when(eligible, value))
    )

    # RANGE theo giay (khong phai ROWS = 30 ban ghi!).
    # [-30 ngay, -1 giay] dam bao KHONG tinh ban ghi hien tai
    # va KHONG nhin bat ky ban ghi tuong lai nao.
    lookback_seconds = lookback_days * 24 * 60 * 60
    history_window = (
        Window.partitionBy("sensor_id", "parameter", "_hour_utc")
        .orderBy("_epoch_s")
        .rangeBetween(-lookback_seconds, -1)
    )

    # Cung gio UTC, lay TOAN BO mau hop le trong 30 ngay truoc.
    # Sap xep mang de lay MEDIAN chinh xac, khong dung xap xi.
    # Voi 30 ngay va AQ hourly: toi da ~30 gia tri/nhom/lan tinh.
    with_samples = prepared.withColumn(
        "_values", F.array_sort(F.collect_list("_valid_sample").over(history_window))
    ).withColumn(
        "sample_count", F.size(F.col("_values")).cast("int")
    )

    n = F.col("sample_count")
    # element_at dung index BAT DAU TU 1.
    # N le: 2 index trung nhau; N chan: trung binh cua 2 so giua.
    lower_index = F.floor((n + F.lit(1)) / F.lit(2)).cast("int")
    upper_index = F.floor((n + F.lit(2)) / F.lit(2)).cast("int")
    median = (
        F.element_at(F.col("_values"), lower_index) / F.lit(2.0)
        + F.element_at(F.col("_values"), upper_index) / F.lit(2.0)
    )

    # Du khong du 10 mau thi VAN GIU dong timestamp de Nguoi 3
    # biet do la baseline KHONG USABLE, khong phai PM2.5 = 0.
    baseline = with_samples.select(
        "sensor_id",
        "parameter",
        "timestamp_utc",
        F.when(n >= F.lit(min_samples), median)
        .otherwise(F.lit(None).cast("double"))
        .cast("double")
        .alias("baseline_ugm3"),
        n.alias("sample_count"),
        F.lit(method).alias("method"),
        (n >= F.lit(min_samples)).alias("is_usable"),
    )
    validate_contract(baseline, root)

    # Data quality sanity check: moi dong AQ co dung mot dong baseline.
    silver_rows = aq.count()
    baseline_rows = baseline.count()
    if silver_rows != baseline_rows:
        raise ValueError(
            f"Unexpected row count: Silver={silver_rows}, baseline={baseline_rows}"
        )

    usable_rows = baseline.filter(F.col("is_usable")).count()
    baseline.write.mode("errorifexists").parquet(output_path.as_posix())

    return {
        "silver_rows": silver_rows,
        "baseline_rows": baseline_rows,
        "usable_baseline_rows": usable_rows,
        "unusable_baseline_rows": baseline_rows - usable_rows,
        "output": output_path.as_posix(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/project.yaml")
    args = parser.parse_args()

    root = Path.cwd()
    config = yaml.safe_load((root / args.config).read_text(encoding="utf-8"))

    spark = (
        SparkSession.builder
        .appName("OpenAQ-Air-Quality-Baseline-Curated")
        .master(config["processing"]["spark_master"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", config["processing"]["spark_driver_memory"])
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("ERROR")
        result = build_baseline(spark, root, config)
        for key, item in result.items():
            print(f"{key}: {item}")
        print("Air quality baseline Curated completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
