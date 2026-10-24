
"""Run Fire Detection on real Curated Parquet and export preview JSON."""

import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.validation import validate_config, validate_events

from src.events.fire_detection import (
    filter_fire_candidates,
    cluster_fire_candidates,
)

from src.events.fire_event_builder import build_fire_events


# 1. Doc Fire Parquet cua Nguoi 2
def read_fire_data(spark, path):

    if not path.is_dir() or not any(path.rglob("*.parquet")):
        raise FileNotFoundError(
            f"Khong tim thay Fire Parquet: {path}"
        )

    df = spark.read.parquet(str(path))

    df = df.select(
        "hotspot_id",
        "timestamp_utc",
        "latitude",
        "longitude",
        "confidence_raw",
        "frp_mw",
        "satellite",
    )

    # Spark timestamp -> ISO UTC string
    df = df.withColumn(
        "timestamp_utc",
        F.date_format(
            F.col("timestamp_utc"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
        )
    )

    return [
        row.asDict(recursive=True)
        for row in df.collect()
    ]


# 2. Ghi Event JSON preview
def save_fire_events(events, config):

    output_dir = ROOT / config["storage"]["demo"]

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_path = (
        output_dir / "events_fire_real_preview.json"
    )

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


# 3. Dieu phoi toan bo Fire pipeline
def main():

    config = validate_config(
        ROOT / "configs/project.yaml"
    )

    fire_path = (
        ROOT
        / config["storage"]["curated"]
        / "fire_hotspots_context"
    )

    spark = (
        SparkSession.builder
        .appName("FireEventRealPreview")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        # A. Doc Parquet
        hotspots = read_fire_data(
            spark,
            fire_path
        )

        print("\n===== REAL FIRE INPUT =====")
        print("Total hotspots:", len(hotspots))

        # B. Loc hotspot
        candidates, counts = filter_fire_candidates(
            hotspots,
            config
        )

        print("\n===== FIRE FILTERING =====")
        print("Accepted:", counts["accepted"])
        print(
            "Rejected confidence:",
            counts["rejected_confidence"]
        )
        print(
            "Rejected invalid:",
            counts["rejected_invalid"]
        )

        if (
            counts["accepted"]
            + counts["rejected_confidence"]
            + counts["rejected_invalid"]
            != counts["total"]
        ):
            raise ValueError(
                "Fire filtering counts do not match"
            )

        # C. ST-DBSCAN
        print("\nRunning ST-DBSCAN...")

        started = time.perf_counter()

        clusters = cluster_fire_candidates(
            candidates,
            config
        )

        elapsed = time.perf_counter() - started

        clustered_count = sum(
            len(cluster)
            for cluster in clusters
        )

        noise_count = len(candidates) - clustered_count

        if noise_count < 0:
            raise ValueError(
                "Clustered hotspots exceed candidates"
            )

        print("\n===== FIRE CLUSTERING =====")
        print("Clusters:", len(clusters))
        print("Hotspots in clusters:", clustered_count)
        print("Noise:", noise_count)
        print("Clustering seconds:", round(elapsed, 2))

        # D. Bien cluster thanh Fire Event
        events = build_fire_events(
            clusters,
            region_id=config["integration"]["region_id"]
        )

        if len(events) != len(clusters):
            raise ValueError(
                "Number of events differs from clusters"
            )

        validated_count = validate_events(events)

        print("\n===== FIRE EVENTS =====")
        print("Events:", len(events))
        print("Validated events:", validated_count)

        # E. Ghi JSON preview
        output_path = save_fire_events(
            events,
            config
        )

        print("\n===== OUTPUT =====")
        print("Saved to:", output_path)

        # F. Hien thi Event dau tien
        if events:

            event = events[0]
            metrics = event["metrics"]

            print("\n===== FIRST FIRE EVENT =====")
            print("ID:", event["event_id"])
            print("Start:", event["start_time_utc"])
            print("End:", event["end_time_utc"])
            print("Latitude:", event["centroid_latitude"])
            print("Longitude:", event["centroid_longitude"])
            print("Hotspots:", metrics["hotspot_count"])
            print("Max FRP:", metrics["max_frp_mw"])
            print("Sum FRP:", metrics["sum_frp_mw"])
            print("Satellites:", metrics["satellites"])
            print("Evidence:", len(event["evidence_ids"]))

        print("\nFire Event preview completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
