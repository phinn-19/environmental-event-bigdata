
"""Inspect real Curated Parquet delivered by Member 2."""

from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "curated"

tables = {
    "air_quality_context_hourly": [
        "sensor_id", "timestamp_utc",
        "value_ugm3", "has_flags"
    ],
    "air_quality_baseline": [
        "sensor_id", "timestamp_utc",
        "baseline_ugm3", "sample_count", "is_usable"
    ],
    "fire_hotspots_context": [
        "hotspot_id", "timestamp_utc",
        "latitude", "longitude", "confidence_raw"
    ],
}

spark = (
    SparkSession.builder
    .appName("InspectCuratedData")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    for table_name, selected_columns in tables.items():
        path = DATA_DIR / table_name

        if not list(path.glob("*.parquet")):
            raise FileNotFoundError(f"Missing Parquet: {path}")

        df = spark.read.parquet(str(path))

        print(f"\n===== {table_name} =====")
        print("Number of rows:", df.count())

        print("\nSchema:")
        df.printSchema()

        print("\nFirst 3 rows:")
        df.select(*selected_columns).show(3, truncate=False)

finally:
    spark.stop()
