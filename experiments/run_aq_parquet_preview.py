
"""Preview AQ Events detected from real Curated Parquet."""

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# 1. Xac dinh thu muc goc du an
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


# 2. Import cac ham da xay dung
from src.common.validation import (
    validate_config,
    validate_events,
)

from src.events.aq_detection import (
    detect_aq_candidates,
    group_consecutive_candidates,
)

from src.events.aq_event_builder import (
    build_aq_events,
)


# 3. Doc Parquet va chuyen doi timestamp
def read_curated(spark, table_name, columns, time_columns):

    path = ROOT / "data" / "curated" / table_name

    if not list(path.glob("*.parquet")):
        raise FileNotFoundError(
            f"Khong tim thay Parquet trong: {path}"
        )

    df = spark.read.parquet(str(path))

    # Chi lay nhung cot thuat toan AQ can
    df = df.select(*columns)

    # Chuyen timestamp thanh chuoi ISO UTC
    for column in time_columns:
        df = df.withColumn(
            column,
            F.date_format(
                F.col(column),
                "yyyy-MM-dd'T'HH:mm:ss'Z'"
            )
        )

    return [
        row.asDict(recursive=True)
        for row in df.collect()
    ]


# 4. Luu Event JSON vao thu muc demo
def save_preview(events, config):

    output_dir = ROOT / config["storage"]["demo"]
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = (
        output_dir / "events_aq_real_preview.json"
    )

    # Kiem tra truoc khi ghi
    validate_events(events)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            events,
            file,
            ensure_ascii=False,
            indent=2,
            allow_nan=False
        )

    return output_path


# 5. Dieu phoi toan bo luong xu ly
def main():

    config = validate_config(
        ROOT / "configs" / "project.yaml"
    )

    spark = (
        SparkSession.builder
        .appName("AQRealEventPreview")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        # BUOC A: Doc du lieu that cua Nguoi 2
        observations = read_curated(
            spark,
            "air_quality_context_hourly",
            [
                "sensor_id",
                "parameter",
                "timestamp_utc",
                "period_end_utc",
                "value_ugm3",
                "has_flags",
                "latitude",
                "longitude"
            ],
            [
                "timestamp_utc",
                "period_end_utc"
            ]
        )

        baselines = read_curated(
            spark,
            "air_quality_baseline",
            [
                "sensor_id",
                "parameter",
                "timestamp_utc",
                "baseline_ugm3",
                "sample_count",
                "method",
                "is_usable"
            ],
            ["timestamp_utc"]
        )

        print("\n===== REAL DATA =====")
        print("Observations:", len(observations))
        print("Baselines:", len(baselines))

        # BUOC B: Phat hien gio bat thuong
        candidates = detect_aq_candidates(
            observations,
            baselines,
            config
        )

        print("\n===== CANDIDATES =====")
        print("Candidates:", len(candidates))

        # BUOC C: Gom cac gio lien tiep
        groups = group_consecutive_candidates(
            candidates,
            config
        )

        print("\n===== EVENT GROUPS =====")
        print("Groups:", len(groups))

        # BUOC D: Xay dung Event JSON
        region_id = config["integration"]["region_id"]

        events = build_aq_events(
            groups,
            region_id=region_id
        )

        # Sap xep Event theo thoi gian bat dau
        events.sort(
            key=lambda event: (
                event["start_time_utc"],
                event["event_id"]
            )
        )

        print("\n===== AQ EVENTS =====")
        print("Events:", len(events))
        print("Validated events:", validate_events(events))

        # BUOC E: Luu Event JSON
        output_path = save_preview(events, config)

        print("\n===== OUTPUT =====")
        print("Saved to:", output_path)

        # In thong tin Event dau tien
        if events:
            first = events[0]

            print("\n===== FIRST EVENT =====")
            print("Event ID:", first["event_id"])
            print("Region:", first["region_id"])
            print("Start:", first["start_time_utc"])
            print("End:", first["end_time_utc"])
            print("Peak:", first["metrics"]["peak_ugm3"])
            print("Mean:", first["metrics"]["mean_ugm3"])
            print("Duration:", first["metrics"]["duration_hours"])
            print("Evidence:", len(first["evidence_ids"]))

        print("\nAQ real-data preview completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
