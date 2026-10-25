
"""CLI for Member 3: run AQ Event Detection on curated Parquet."""

import argparse
import json
from pathlib import Path

from src.common.validation import (
    validate_config,
    validate_events,
)

from src.events.aq_detection import (
    detect_aq_candidates,
    group_consecutive_candidates,
)

from src.events.aq_event_builder import build_aq_events


# 1. Thu muc goc du an
ROOT = Path(__file__).resolve().parents[2]


# 2. Doc du lieu Parquet tu Nguoi 2
def read_parquet_rows(spark, directory, columns, time_columns):
    """Read Parquet and convert Spark timestamps to UTC strings."""

    from pyspark.sql import functions as F

    if not directory.is_dir():
        raise FileNotFoundError(f"Missing directory: {directory}")

    if not any(directory.rglob("*.parquet")):
        raise FileNotFoundError(f"Missing Parquet files: {directory}")

    df = spark.read.parquet(str(directory))

    # Chi lay nhung cot can thiet
    df = df.select(*columns)

    # Chuyen timestamp thanh ISO UTC string
    for column in time_columns:
        df = df.withColumn(
            column,
            F.date_format(
                F.col(column),
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
            ),
        )

    # Phu hop cho bo du lieu preview nho
    return [
        row.asDict(recursive=True)
        for row in df.collect()
    ]


# 3. Chay toan bo AQ pipeline
def run_aq_preview(config_path):
    """Detect AQ events and save an AQ-only preview JSON."""

    from pyspark.sql import SparkSession

    config = validate_config(config_path)

    curated_dir = ROOT / config["storage"]["curated"]
    output_dir = ROOT / config["storage"]["demo"]

    output_path = output_dir / "events_aq_real_preview.json"

    # Khoi tao Spark
    spark = (
        SparkSession.builder
        .appName("Member3-AQ-Preview")
        .master(config["processing"]["spark_master"])
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # A. Doc bang AQ Context
        observations = read_parquet_rows(
            spark,
            curated_dir / "air_quality_context_hourly",
            [
                "sensor_id",
                "parameter",
                "timestamp_utc",
                "period_end_utc",
                "value_ugm3",
                "has_flags",
                "latitude",
                "longitude",
            ],
            ["timestamp_utc", "period_end_utc"],
        )

        # B. Doc bang AQ Baseline
        baselines = read_parquet_rows(
            spark,
            curated_dir / "air_quality_baseline",
            [
                "sensor_id",
                "parameter",
                "timestamp_utc",
                "baseline_ugm3",
                "sample_count",
                "method",
                "is_usable",
            ],
            ["timestamp_utc"],
        )

        print("\n===== REAL AQ INPUT =====")
        print("Observations:", len(observations))
        print("Baselines:", len(baselines))

        # C. Phat hien candidate
        candidates = detect_aq_candidates(
            observations,
            baselines,
            config,
        )

        print("\n===== CANDIDATES =====")
        print("Candidates:", len(candidates))

        # D. Gom candidate lien tiep
        groups = group_consecutive_candidates(
            candidates,
            config,
        )

        print("\n===== EVENT GROUPS =====")
        print("Groups:", len(groups))

        # E. Tao Event JSON
        events = build_aq_events(
            groups,
            region_id=config["integration"]["region_id"],
        )

        events.sort(
            key=lambda event: (
                event["start_time_utc"],
                event["event_id"],
            )
        )

        # F. Kiem tra Event Contract
        valid_count = validate_events(events)

        print("\n===== AQ EVENTS =====")
        print("Events:", len(events))
        print("Validated events:", valid_count)

        # G. Ghi JSON preview
        output_dir.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(
                events,
                file,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )

        print("\n===== OUTPUT =====")
        print("Saved to:", output_path)
        print("\nAQ CLI preview completed.")

        return 0

    finally:
        spark.stop()


# 4. Nhan lenh tu terminal
def main(argv=None):

    parser = argparse.ArgumentParser(
        description="Member 3 - Event Detection CLI"
    )

    parser.add_argument(
        "command",
        choices=["aq-preview"],
        help="Run AQ detection using curated Parquet",
    )

    parser.add_argument(
        "--config",
        default="configs/project.yaml",
        help="Path to project configuration",
    )

    args = parser.parse_args(argv)

    config_path = Path(args.config)

    if not config_path.is_absolute():
        config_path = ROOT / config_path

    if args.command == "aq-preview":
        return run_aq_preview(config_path)

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
