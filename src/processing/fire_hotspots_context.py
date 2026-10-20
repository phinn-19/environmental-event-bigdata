"""Build Curated fire_hotspots_context: NASA FIRMS Silver + ERA5 Silver.

Data Contract v1, section 3.2 (18 columns, unchanged):
- LEFT JOIN keeps every FIRMS hotspot, including observations without weather.
- Nearest grid point is restricted to the available ERA5 grid-cell coverage.
- FIRMS gives an *instant* (often minute precision), unlike hourly OpenAQ:
  temperature and wind use the nearest AVAILABLE ERA5 validity hour, with
  at most half an hour offset (the inherent limit of hourly time matching).
- ERA5 rainfall at validity H represents the accumulation for (H-1h, H].
  For a hotspot at 09:35, report the 09:00-10:00 rain (validity 10:00),
  at the SAME selected grid point. For hotspot at exactly 09:00, use
  the 09:00-10:00 hour as well (the forward hourly bucket convention).
- If spatial / temporal / rainfall context is missing, use NULL, never 0.

Implementation policies (not new Data Contract columns):
- Spatial coverage is the half-grid-cell footprint encoded in grid_id.
- Optional run-time caps --max-distance-km and --max-time-offset-minutes
  let the team set STRICTER limits without editing project.yaml/contract.
  No user-defined distance cutoff is assumed by default; half-grid-cell
  geometry protects VN-wide hotspots from being assigned HANOI-only ERA5.
- The 30-minute upper bound is implied by hourly nearest-time matching, not
  an empirically validated weather accuracy threshold. Please document it.

This job does not infer actual fires, fire clusters, or causal air pollution.
"""

import argparse
import json
import math
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, Window, functions as F


# FIRMS Silver fields in the exact contract order.
FIRE_COLS = [
    "hotspot_id", "timestamp_utc", "latitude", "longitude", "frp_mw",
    "confidence_raw", "satellite", "instrument", "daynight",
    "source_product", "source",
]

# ERA5 Silver fields in the exact contract order; dewpoint is not part of
# fire context, so do not add it silently to the curated output.
WX_COLS = [
    "grid_id", "timestamp_utc", "latitude", "longitude",
    "temperature_c", "dewpoint_c", "wind_u_ms", "wind_v_ms",
    "precipitation_mm_1h", "source",
]

CONTEXT_COLS = [
    "weather_grid_id", "weather_timestamp_utc", "weather_distance_km",
    "temperature_c", "wind_u_ms", "wind_v_ms", "precipitation_mm_1h",
]


def check_unique(df, keys, name):
    """Stop if an input/output key is null or duplicated (no silent fanout)."""
    for key in keys:
        if df.filter(F.col(key).isNull()).limit(1).count():
            raise ValueError(f"{name}: null key {key}")
    if df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{name}: duplicate key {keys}")


def check_coordinates(df, name):
    """EPSG:4326 requires finite latitude/longitude in legal ranges."""
    lat, lon = F.col("latitude"), F.col("longitude")
    invalid = (
        lat.isNull() | lon.isNull() | F.isnan(lat) | F.isnan(lon)
        | ~lat.between(-90.0, 90.0) | ~lon.between(-180.0, 180.0)
    )
    if df.filter(invalid).limit(1).count():
        raise ValueError(f"{name}: invalid coordinates")


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km; preserve NULL when ERA5 is absent.

    Spark greatest(0, NULL) = 0, so checking validity BEFORE clipping h
    is essential; otherwise a missing weather grid incorrectly becomes 0 km.
    """
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dlat = p2 - p1
    dlon = F.radians(lon2 - lon1)
    h = (
        F.pow(F.sin(dlat / F.lit(2.0)), F.lit(2.0))
        + F.cos(p1) * F.cos(p2)
        * F.pow(F.sin(dlon / F.lit(2.0)), F.lit(2.0))
    )
    h_safe = F.least(F.lit(1.0), F.greatest(F.lit(0.0), h))
    dist = F.lit(2.0 * 6371.0088) * F.asin(F.sqrt(h_safe))
    valid = (
        lat1.isNotNull() & lon1.isNotNull()
        & lat2.isNotNull() & lon2.isNotNull()
    )
    return F.when(valid, dist).otherwise(F.lit(None).cast("double"))


def validate_contract(df, root):
    """Use the repo's JSON contract (order and types), never replace it."""
    path = root / "contracts/schemas/table_contracts.json"
    obj = json.loads(path.read_text(encoding="utf-8"))
    fields = obj["tables"]["fire_hotspots_context"]["fields"]
    if list(fields) != FIRE_COLS + CONTEXT_COLS or df.columns != list(fields):
        raise ValueError("Fire context column names/order differ from contract")
    for name, declared in fields.items():
        expected = declared.removesuffix("?").replace("utc", "timestamp")
        actual = df.schema[name].dataType.simpleString()
        if actual != expected:
            raise ValueError(f"Fire context {name}: {actual} != {expected}")


def build_context(
    spark, root, config, *, max_distance_km=None,
    max_time_offset_minutes=None,
):
    """Return metrics after writing new Curated Parquet; never overwrite.

    max_distance_km / max_time_offset_minutes are optional stricter run caps,
    not unreviewed defaults placed in the group project contract.
    """
    if config["project"]["timezone"] != "UTC" or config["processing"]["timezone"] != "UTC":
        raise ValueError("FIRMS/ERA5 context requires UTC")
    if config["project"]["crs"] != "EPSG:4326":
        raise ValueError("FIRMS/ERA5 context requires EPSG:4326")
    for value, label in (
        (max_distance_km, "max_distance_km"),
        (max_time_offset_minutes, "max_time_offset_minutes"),
    ):
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError(f"{label} must be a finite positive number")

    data_root = Path(config["project"]["data_root"])
    if data_root.is_absolute() or ".." in data_root.parts:
        raise ValueError("data_root must be relative to repo and not contain '..'")
    fire_path = root / data_root / "silver/fire_hotspots"
    wx_path = root / data_root / "silver/weather_hourly"
    out_path = root / data_root / "curated/fire_hotspots_context"

    for path in (fire_path, wx_path):
        if not path.exists():
            raise FileNotFoundError(f"Missing Silver: {path}")
    if out_path.exists():
        raise FileExistsError(f"Curated output already exists: {out_path}")

    fire = spark.read.parquet(fire_path.as_posix())
    wx = spark.read.parquet(wx_path.as_posix())
    if fire.columns != FIRE_COLS or wx.columns != WX_COLS:
        raise ValueError("Silver columns/order changed; check Data Contract")

    check_unique(fire, ["hotspot_id"], "FIRMS")
    check_unique(wx, ["grid_id", "timestamp_utc"], "ERA5")
    check_coordinates(fire, "FIRMS")
    check_coordinates(wx, "ERA5")
    if fire.filter(F.col("timestamp_utc").isNull()).limit(1).count():
        raise ValueError("FIRMS: null observation timestamp")
    if wx.filter(F.col("timestamp_utc").isNull()).limit(1).count():
        raise ValueError("ERA5: null validity timestamp")

    # ERA5 grid_id is produced by the Silver processor, for example:
    # era5:regular_ll:0.250000:0.250000:21.000000:105.750000
    # The two resolution numbers define half-cell spatial coverage.
    wx = (
        wx.withColumn("_dlat", F.split("grid_id", ":").getItem(2).cast("double"))
        .withColumn("_dlon", F.split("grid_id", ":").getItem(3).cast("double"))
    )
    invalid_grid = (
        ~F.col("grid_id").rlike(
            r"^era5:regular_ll:[0-9.]+:[0-9.]+:[-0-9.]+:[-0-9.]+$"
        )
        | F.col("_dlat").isNull() | F.col("_dlon").isNull()
        | F.isnan("_dlat") | F.isnan("_dlon")
        | ~F.col("_dlat").between(0.000001, 5.0)
        | ~F.col("_dlon").between(0.000001, 5.0)
    )
    if wx.filter(invalid_grid).limit(1).count():
        raise ValueError("Invalid/unsupported ERA5 grid_id resolution")

    # Hotspot is an instantaneous satellite observation, e.g. 09:35 UTC.
    # Candidate validity times are 09:00 and 10:00, not every ERA5 row.
    # They are generated as separate rows to enable equality join on time.
    # For rain we always need the containing hour [09:00,10:00), so the
    # rain validity timestamp will be 10:00 regardless of nearest instant.
    fire_hours = (
        fire.withColumn("_bucket_start", F.date_trunc("hour", "timestamp_utc"))
        .withColumn("_bucket_end", F.expr("_bucket_start + INTERVAL 1 HOUR"))
        .withColumn(
            "_candidate_time",
            F.explode(F.array(F.col("_bucket_start"), F.col("_bucket_end"))),
        )
    )

    # Longitude wrap accounts for +-180 degrees; latitude is normal degrees.
    # A hotspot outside the ERA5 HANOI tile must NEVER borrow HANOI weather.
    lon_delta = (
        F.pmod(F.col("f.longitude") - F.col("w.longitude") + F.lit(180.0),
               F.lit(360.0)) - F.lit(180.0)
    )
    in_cell = (
        (F.abs(F.col("f.latitude") - F.col("w.latitude"))
         <= F.col("w._dlat") / F.lit(2.0) + F.lit(1e-8))
        & (F.abs(lon_delta)
           <= F.col("w._dlon") / F.lit(2.0) + F.lit(1e-8))
    )
    offset_seconds = F.abs(
        F.unix_timestamp(F.col("f.timestamp_utc"))
        - F.unix_timestamp(F.col("w.timestamp_utc"))
    )
    # Half the sampling period = 30 minutes. User can impose a STRICTER
    # time cap, but no future/past hour beyond the nearest-hour window.
    allowed_seconds = 1800.0
    if max_time_offset_minutes is not None:
        allowed_seconds = min(allowed_seconds, 60.0 * max_time_offset_minutes)

    condition = (
        (F.col("f._candidate_time") == F.col("w.timestamp_utc"))
        & (offset_seconds <= F.lit(allowed_seconds + 1e-7))
        & in_cell
    )
    if max_distance_km is not None:
        # Extra cap belongs in the LEFT JOIN condition, NEVER in a WHERE
        # filter afterwards (which could accidentally delete a hotspot).
        condition = condition & (
            haversine_km(
                F.col("f.latitude"), F.col("f.longitude"),
                F.col("w.latitude"), F.col("w.longitude"),
            ) <= F.lit(max_distance_km)
        )

    joined = (
        fire_hours.alias("f")
        .join(wx.alias("w"), condition, "left")
        .withColumn(
            "_distance_km",
            haversine_km(
                F.col("f.latitude"), F.col("f.longitude"),
                F.col("w.latitude"), F.col("w.longitude"),
            ),
        )
        .withColumn("_time_offset_s", offset_seconds.cast("double"))
    )

    # Choose the closest *available hour* first. Within that hour, choose
    # the nearest grid (km). Equal offsets prefer earlier hour; equal grid
    # distances prefer stable grid_id, so repeated runs are reproducible.
    rank = Window.partitionBy(F.col("f.hotspot_id")).orderBy(
        F.col("_time_offset_s").asc_nulls_last(),
        F.col("_distance_km").asc_nulls_last(),
        F.col("w.timestamp_utc").asc_nulls_last(),
        F.col("w.grid_id").asc_nulls_last(),
    )
    nearest = joined.withColumn("_rn", F.row_number().over(rank)).filter(
        F.col("_rn") == 1
    )

    instant = nearest.select(
        *[F.col("f." + name).alias(name) for name in FIRE_COLS],
        F.col("f._bucket_end").alias("_rain_end_utc"),
        F.col("w.grid_id").alias("weather_grid_id"),
        F.col("w.timestamp_utc").alias("weather_timestamp_utc"),
        F.col("_distance_km").cast("double").alias("weather_distance_km"),
        F.col("w.temperature_c").alias("temperature_c"),
        F.col("w.wind_u_ms").alias("wind_u_ms"),
        F.col("w.wind_v_ms").alias("wind_v_ms"),
    )

    # FIRMS has no hourly observation interval. Explicitly choose the UTC
    # bucket containing the detection, and the precipitation ending at its
    # upper boundary. Rain is not an instantaneous measurement at the fire.
    # Join to the SAME selected grid, even if T/wind used the previous hour.
    rain = wx.select(
        F.col("grid_id").alias("_rain_grid_id"),
        F.col("timestamp_utc").alias("_rain_validity_end"),
        F.col("precipitation_mm_1h").alias("_rain_mm"),
    )
    with_rain = instant.join(
        rain,
        (F.col("weather_grid_id") == F.col("_rain_grid_id"))
        & (F.col("_rain_end_utc") == F.col("_rain_validity_end")),
        "left",
    ).withColumn("precipitation_mm_1h", F.col("_rain_mm").cast("double"))

    # Output EXACTLY the 18 columns and Spark types in table_contracts.json.
    contract = json.loads(
        (root / "contracts/schemas/table_contracts.json").read_text(encoding="utf-8")
    )
    declared = list(contract["tables"]["fire_hotspots_context"]["fields"])
    result = with_rain.select(*declared)
    validate_contract(result, root)

    fire_count = fire.count()
    result_count = result.count()
    if result_count != fire_count:
        raise ValueError(f"LEFT JOIN changed FIRMS rows {fire_count}->{result_count}")
    check_unique(result, ["hotspot_id"], "FIRMS context")

    matched = result.filter(F.col("weather_grid_id").isNotNull()).count()
    rain_matched = result.filter(F.col("precipitation_mm_1h").isNotNull()).count()
    result.write.mode("errorifexists").parquet(out_path.as_posix())
    return {
        "fire_hotspots_rows": fire_count,
        "context_rows": result_count,
        "weather_matched_rows": matched,
        "weather_missing_rows": result_count - matched,
        "precipitation_matched_rows": rain_matched,
        "output": out_path.as_posix(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/project.yaml")
    parser.add_argument("--max-distance-km", type=float, default=None)
    parser.add_argument("--max-time-offset-minutes", type=float, default=None)
    args = parser.parse_args()

    root = Path.cwd()
    config = yaml.safe_load((root / args.config).read_text(encoding="utf-8"))
    spark = (
        SparkSession.builder
        .appName("Fire-Hotspots-Weather-Context-Curated")
        .master(config["processing"]["spark_master"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", config["processing"]["spark_driver_memory"])
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("ERROR")
        stats = build_context(
            spark, root, config,
            max_distance_km=args.max_distance_km,
            max_time_offset_minutes=args.max_time_offset_minutes,
        )
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("Fire hotspots context Curated completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
