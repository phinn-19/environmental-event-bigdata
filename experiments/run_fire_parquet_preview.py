
"""Preview Fire clusters from real Curated Parquet data."""

import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# 1. Xac dinh thu muc goc du an
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.validation import validate_config
from src.events.fire_detection import (
    filter_fire_candidates,
    cluster_fire_candidates,
)


# 2. Doc du lieu Fire Parquet
def read_fire_parquet(spark, path):
    """Read selected FIRMS columns and normalize UTC timestamp."""

    if not path.is_dir() or not any(path.rglob("*.parquet")):
        raise FileNotFoundError(
            f"Khong tim thay Fire Parquet: {path}"
        )

    df = spark.read.parquet(str(path))

    # Chi lay nhung cot Fire Detection can
    df = df.select(
        "hotspot_id",
        "timestamp_utc",
        "latitude",
        "longitude",
        "frp_mw",
        "confidence_raw",
        "satellite",
    )

    # Chuyen Spark timestamp thanh ISO UTC string
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


# 3. Ham chinh dieu phoi qua trinh
def main():

    config = validate_config(
        ROOT / "configs" / "project.yaml"
    )

    fire_path = (
        ROOT
        / config["storage"]["curated"]
        / "fire_hotspots_context"
    )

    spark = (
        SparkSession.builder
        .appName("FireRealDataPreview")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # BUOC A: Doc du lieu tu Nguoi 2
        hotspots = read_fire_parquet(
            spark,
            fire_path
        )

        print("\n===== REAL FIRE INPUT =====")
        print("Total hotspots:", len(hotspots))

        # BUOC B: Loc candidate
        candidates, counts = filter_fire_candidates(
            hotspots,
            config
        )

        print("\n===== FIRE FILTERING =====")
        print("Total:", counts["total"])
        print("Accepted:", counts["accepted"])
        print(
            "Rejected confidence:",
            counts["rejected_confidence"]
        )
        print(
            "Rejected invalid:",
            counts["rejected_invalid"]
        )

        # BUOC C: Phan cum bang ST-DBSCAN
        print("\nDang phan cum ST-DBSCAN...")

        started = time.perf_counter()

        clusters = cluster_fire_candidates(
            candidates,
            config
        )

        elapsed = time.perf_counter() - started

        # BUOC D: Thong ke ket qua
        clustered_count = sum(
            len(cluster)
            for cluster in clusters
        )

        noise_count = len(candidates) - clustered_count

        # Moi hotspot chi duoc thuoc toi da mot cum
        assigned_ids = [
            row["hotspot_id"]
            for cluster in clusters
            for row in cluster
        ]

        if len(assigned_ids) != len(set(assigned_ids)):
            raise ValueError(
                "Mot hotspot bi gan vao nhieu cum!"
            )

        if noise_count < 0:
            raise ValueError(
                "So hotspot trong cum vuot qua candidates!"
            )

        print("\n===== FIRE CLUSTERING =====")
        print("Clusters:", len(clusters))
        print("Hotspots in clusters:", clustered_count)
        print("Noise hotspots:", noise_count)
        print("Clustering time (seconds):", round(elapsed, 2))

        # BUOC E: Hien thi toi da 5 cum lon nhat
        largest_clusters = sorted(
            clusters,
            key=lambda group: (
                -len(group),
                min(row["timestamp_utc"] for row in group)
            )
        )

        print("\n===== LARGEST CLUSTERS =====")

        if not largest_clusters:
            print("Khong tim thay cum du dieu kien.")

        for index, cluster in enumerate(
            largest_clusters[:5],
            start=1
        ):
            times = [
                row["timestamp_utc"]
                for row in cluster
            ]

            print(f"\nCluster {index}")
            print("Hotspot count:", len(cluster))
            print("First observation:", min(times))
            print("Last observation:", max(times))
            print(
                "Sample IDs:",
                [row["hotspot_id"][:12] for row in cluster[:3]]
            )

        print("\nFire clustering preview completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
