
"""NASA FIRMS raw CSV -> Silver fire_hotspots (contract v1)."""

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, functions as F


# Cac cot trong CSV goc cua NASA FIRMS
RAW_COLUMNS = [
    "latitude", "longitude", "bright_ti4", "scan", "track",
    "acq_date", "acq_time", "satellite", "instrument", "confidence",
    "version", "bright_ti5", "frp", "daynight", "type",
]


def verified_firms_files(root: Path, data_root: str):
    """Chi lay CSV khop voi manifest."""
    raw_root = (root / data_root / "raw/nasa_firms").resolve()
    manifests = sorted(
        (raw_root / "manifests").rglob("*.manifest.json")
    )

    if not manifests:
        raise FileNotFoundError("No NASA FIRMS manifests found")

    paths, total, header = [], 0, None

    for manifest_path in manifests:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        if (
            manifest.get("schema_version") != "manifest-v1"
            or manifest.get("source") != "nasa_firms"
            or manifest.get("status") not in ("success", "empty")
            or manifest.get("format") != "csv"
            or manifest.get("source_product") not in (
                "VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT"
            )
        ):
            raise ValueError(f"Invalid FIRMS manifest: {manifest_path}")

        raw_file = (root / manifest["file"]).resolve()

        if (
            not raw_file.is_relative_to(raw_root)
            or not raw_file.is_file()
            or raw_file.relative_to(raw_root).parts[0]
            != manifest["source_product"]
        ):
            raise ValueError(f"Invalid FIRMS file path: {raw_file}")

        content = raw_file.read_bytes()

        if (
            len(content) != manifest.get("bytes")
            or hashlib.sha256(content).hexdigest()
            != manifest.get("sha256")
        ):
            raise ValueError(f"FIRMS checksum mismatch: {raw_file}")

        reader = csv.DictReader(
            io.StringIO(content.decode("utf-8-sig"))
        )

        if (
            reader.fieldnames is None
            or any(c not in reader.fieldnames for c in RAW_COLUMNS)
        ):
            raise ValueError(f"FIRMS header incomplete: {raw_file}")

        if header is not None and reader.fieldnames != header:
            raise ValueError("FIRMS CSV headers differ between files")

        header = reader.fieldnames
        count = sum(1 for _ in reader)

        if count != manifest.get("row_count"):
            raise ValueError(f"FIRMS row count mismatch: {raw_file}")

        if manifest["status"] == "empty" and count:
            raise ValueError("Empty FIRMS manifest contains rows")

        paths.append(raw_file.as_posix())
        total += count

    return paths, total


def build_silver(spark, root: Path, data_root="data"):
    """Convert verified FIRMS CSVs into Silver Parquet."""

    paths, input_rows = verified_firms_files(root, data_root)

    output = root / data_root / "silver/fire_hotspots"

    if output.exists():
        raise FileExistsError(f"Output already exists: {output}")

    # 1. Spark doc CSV
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .csv(paths)
    )

    df = raw.select(
        *[F.trim(F.col(c)).alias(c) for c in RAW_COLUMNS],
        F.input_file_name().alias("raw_file"),
    )

    # 2. Chuan hoa thoi gian, toa do, FRP
    df = (
        df
        .withColumn(
            "source_product",
            F.regexp_extract(
                "raw_file", r"nasa_firms/([^/]+)/", 1
            )
        )
        .withColumn(
            "latitude_num", F.col("latitude").cast("double")
        )
        .withColumn(
            "longitude_num", F.col("longitude").cast("double")
        )
        .withColumn(
            "frp_num", F.col("frp").cast("double")
        )
        .withColumn(
            "timestamp_utc",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("acq_date"),
                    F.lpad(F.col("acq_time"), 4, "0"),
                ),
                "yyyy-MM-dd HHmm",
            )
        )
    )

    # 3. Kiem tra cac gia tri bat buoc
    bad = (
        (F.col("source_product") == "")
        | F.col("satellite").isNull()
        | (F.col("satellite") == "")
        | ~F.col("acq_time").rlike(r"^\d{1,4}$")
        | F.col("timestamp_utc").isNull()
        | F.col("latitude_num").isNull()
        | F.col("longitude_num").isNull()
        | ~F.col("latitude_num").between(-90, 90)
        | ~F.col("longitude_num").between(-180, 180)
        | (
            F.col("frp").isNotNull()
            & (F.col("frp") != "")
            & F.col("frp_num").isNull()
        )
        | F.isnan("frp_num")
    )

    invalid = df.filter(bad).count()

    if invalid:
        raise ValueError(
            f"{invalid} invalid FIRMS rows; refusing silent data loss"
        )

    # 4. Tao ID on dinh theo hotspot-key-v1
    id_parts = [
        F.col("source_product"),
        F.col("satellite"),
        F.date_format(
            "timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ),
        F.format_string("%.5f", F.col("latitude_num")),
        F.format_string("%.5f", F.col("longitude_num")),
        *[
            F.coalesce(F.col(c), F.lit(""))
            for c in (
                "version", "bright_ti4",
                "scan", "track", "frp"
            )
        ],
    ]

    # 5. Tao bang Silver dung 11 cot
    silver = df.select(
        F.sha2(
            F.concat_ws("|", *id_parts), 256
        ).alias(
            "hotspot_id",
            metadata={"id_algorithm_version": "hotspot-key-v1"},
        ),
        F.col("timestamp_utc"),
        F.col("latitude_num").alias("latitude"),
        F.col("longitude_num").alias("longitude"),
        F.col("frp_num").alias("frp_mw"),
        F.when(
            F.col("confidence") != "",
            F.col("confidence"),
        ).alias("confidence_raw"),
        F.col("satellite"),
        F.when(
            F.col("instrument") != "",
            F.col("instrument"),
        ).alias("instrument"),
        F.when(
            F.col("daynight") != "",
            F.col("daynight"),
        ).alias("daynight"),
        F.col("source_product"),
        F.lit("nasa_firms").alias("source"),
    )

    # 6. Dem ban ghi trung, khong gom cum diem chay
    distinct_rows = silver.dropDuplicates(silver.columns)

    conflicts = (
        distinct_rows
        .groupBy("hotspot_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if conflicts:
        raise ValueError(
            f"{conflicts} IDs have conflicting FIRMS records"
        )

    duplicates = silver.count() - distinct_rows.count()

    # 7. Doi chieu schema voi contract
    contract_path = (
        root / "contracts/schemas/table_contracts.json"
    )
    contract = json.loads(
        contract_path.read_text(encoding="utf-8")
    )
    fields = contract["tables"]["fire_hotspots"]["fields"]

    if distinct_rows.columns != list(fields):
        raise ValueError("FIRMS columns differ from Data Contract")

    for col, contract_type in fields.items():
        typ = contract_type.rstrip("?")
        if typ == "utc":
            typ = "timestamp"

        if distinct_rows.schema[col].dataType.simpleString() != typ:
            raise ValueError(f"Wrong FIRMS field type: {col}")

    # 8. Ghi Parquet
    silver_rows = distinct_rows.count()
    null_frp = distinct_rows.filter(
        F.col("frp_mw").isNull()
    ).count()

    distinct_rows.write.mode("errorifexists").parquet(
        str(output)
    )

    return {
        "input_rows": input_rows,
        "duplicates_removed": duplicates,
        "silver_rows": silver_rows,
        "null_frp": null_frp,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", default="configs/project.yaml"
    )
    args = parser.parse_args()

    root = Path.cwd()

    config = yaml.safe_load(
        (root / args.config).read_text(encoding="utf-8")
    )

    if (
        config["project"]["timezone"] != "UTC"
        or config["processing"]["timezone"] != "UTC"
    ):
        raise ValueError("Data Contract requires UTC")

    data_root = config["project"]["data_root"]

    if (
        Path(data_root).is_absolute()
        or ".." in Path(data_root).parts
    ):
        raise ValueError("data_root must be repo-relative")

    spark = (
        SparkSession.builder
        .appName("FIRMS-Silver")
        .master(config["processing"]["spark_master"])
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.driver.memory",
            config["processing"]["spark_driver_memory"],
        )
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        stats = build_silver(spark, root, data_root)

        for key, value in stats.items():
            print(f"{key}: {value}")

        print("FIRMS Silver completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
