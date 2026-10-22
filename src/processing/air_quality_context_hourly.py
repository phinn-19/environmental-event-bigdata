"""Build Curated air_quality_context_hourly from OpenAQ and ERA5 Silver.

Contract v1, section 3.1:
- Preserve EVERY OpenAQ Silver observation (LEFT JOIN).
- Temperature/wind: nearest ERA5 grid cell at AQ start time t (UTC).
- Precipitation: SAME grid cell, ERA5 validity time t + 1 hour,
  because ERA5 Silver precipitation is the accumulation ENDING at validity time.
- Missing spatial/temporal context produces NULL, never a fabricated zero.

A conservative spatial guard uses the half-cell extent encoded in the grid_id
made by src.processing.era5.grid_id_of(). This prevents an AQ sensor far outside
the available ERA5 tile (e.g. a sensor in HCM vs an ERA5 HANOI tile) from
being assigned distant, misleading weather. This guard is an implementation
choice to document/review with the team, NOT a new Data Contract column.

All significant transformations below are commented in Vietnamese (ASCII).
"""

import argparse
import json
from pathlib import Path

import yaml
from pyspark.sql import SparkSession, Window, functions as F


# 10 cot co san trong OpenAQ Silver, dung thu tu theo Data Contract.
AQ_COLS = [
    "sensor_id", "location_id", "timestamp_utc", "period_end_utc",
    "latitude", "longitude", "parameter", "value_ugm3", "has_flags",
    "source",
]

# 7 cot bo sung theo DATA_CONTRACT_V1.md muc 3.1. KHONG them baseline
# hay acquisition_mode vao bang nay vi chung khong thuoc schema context.
CONTEXT_COLS = [
    "weather_grid_id", "weather_timestamp_utc", "weather_distance_km",
    "temperature_c", "wind_u_ms", "wind_v_ms", "precipitation_mm_1h",
]

WX_COLS = [
    "grid_id", "timestamp_utc", "latitude", "longitude",
    "temperature_c", "dewpoint_c", "wind_u_ms", "wind_v_ms",
    "precipitation_mm_1h", "source",
]


def check_unique(df, keys, label):
    """Bao loi neu key NULL hoac trung; tranh join lam nhan ban quan trac."""
    if df.filter(F.col(keys[0]).isNull()).limit(1).count():
        raise ValueError(f"{label}: null primary key ({keys[0]})")
    for key in keys[1:]:
        if df.filter(F.col(key).isNull()).limit(1).count():
            raise ValueError(f"{label}: null primary key ({key})")
    if (
        df.groupBy(*keys).count().filter(F.col("count") > 1)
        .limit(1).count()
    ):
        raise ValueError(f"{label}: duplicate primary keys {keys}")


def check_coordinates(df, label):
    """Silver phai co toa do huu han, nam trong pham vi EPSG:4326."""
    lat = F.col("latitude")
    lon = F.col("longitude")
    invalid = (
        lat.isNull() | lon.isNull() | F.isnan(lat) | F.isnan(lon)
        | ~lat.between(-90.0, 90.0) | ~lon.between(-180.0, 180.0)
    )
    if df.filter(invalid).limit(1).count():
        raise ValueError(f"{label}: invalid latitude/longitude")


def haversine_km(lat_a, lon_a, lat_b, lon_b):
    """Khoang cach cung lon tren qua dat (km), dung de chon grid gan nhat.

    Earth mean radius = 6371.0088 km (xap xi theo mat cau).
    Khoang cach chi phuc vu chon nearest grid, KHONG la khoang cach lan truyen
    o nhiem va KHONG chung minh quan he nhan qua.
    """
    p1 = F.radians(lat_a)
    p2 = F.radians(lat_b)
    dp = p2 - p1
    dl = F.radians(lon_b - lon_a)
    h = (
        F.pow(F.sin(dp / F.lit(2.0)), F.lit(2.0))
        + F.cos(p1) * F.cos(p2)
        * F.pow(F.sin(dl / F.lit(2.0)), F.lit(2.0))
    )
    # Han che sai so dau phay dong: asin(sqrt(h)) chi hop le khi 0 <= h <= 1.
    # CAN THAN: Spark greatest(0, NULL) tra ve 0 (bo qua NULL), nen neu
    # LEFT JOIN khong tim duoc weather, tinh thang se bi sai thanh 0 km.
    # Bao ve tinh NULL truoc khi kep h vao [0, 1]: khong co grid thi
    # weather_distance_km PHAI la NULL, khong phai khoang cach 0 km.
    valid_pair = (
        lat_a.isNotNull() & lon_a.isNotNull()
        & lat_b.isNotNull() & lon_b.isNotNull()
    )
    h_safe = F.least(F.lit(1.0), F.greatest(F.lit(0.0), h))
    distance = F.lit(2.0 * 6371.0088) * F.asin(F.sqrt(h_safe))
    return F.when(valid_pair, distance).otherwise(F.lit(None).cast("double"))


def validate_contract(df, root):
    """Khong thay doi contract: lay thu tu/type truc tiep tu repo."""
    contract_path = root / "contracts/schemas/table_contracts.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    fields = contract["tables"]["air_quality_context_hourly"]["fields"]

    if set(fields) != set(AQ_COLS + CONTEXT_COLS):
        raise ValueError(
            "Expected context fields differ from Data Contract; stop and review"
        )
    if df.columns != list(fields):
        raise ValueError(f"Context column order differs from contract: {df.columns}")
    for name, declared in fields.items():
        expected = declared.removesuffix("?")
        if expected == "utc":
            expected = "timestamp"
        actual = df.schema[name].dataType.simpleString()
        if expected != actual:
            raise ValueError(f"Context {name}: {actual} != contract {expected}")


def build_context(spark, root, config):
    """Produce context without overwriting any Silver or prior Curated output."""
    if (
        config["project"]["timezone"] != "UTC"
        or config["processing"]["timezone"] != "UTC"
    ):
        raise ValueError("Context join requires UTC")

    data_root = config["project"]["data_root"]
    if Path(data_root).is_absolute() or ".." in Path(data_root).parts:
        raise ValueError("data_root must be repo-relative")

    aq_path = root / data_root / "silver/air_quality_hourly"
    wx_path = root / data_root / "silver/weather_hourly"
    output_path = root / data_root / "curated/air_quality_context_hourly"

    for path in (aq_path, wx_path):
        if not path.exists():
            raise FileNotFoundError(f"Missing Silver input: {path}")
    if output_path.exists():
        raise FileExistsError(f"Curated output already exists: {output_path}")

    aq = spark.read.parquet(aq_path.as_posix())
    wx = spark.read.parquet(wx_path.as_posix())

    if aq.columns != AQ_COLS or wx.columns != WX_COLS:
        raise ValueError("Silver input columns changed; review Silver contract")

    check_unique(aq, ["sensor_id", "parameter", "timestamp_utc"], "OpenAQ")
    check_unique(wx, ["grid_id", "timestamp_utc"], "ERA5")
    check_coordinates(aq, "OpenAQ")
    check_coordinates(wx, "ERA5")

    # Vi du grid_id: era5:regular_ll:0.250000:0.250000:21.000000:105.750000
    # Hai so sau 'regular_ll' la buoc luoi theo latitude/longitude (do).
    # Dung 1/2 buoc luoi lam vung hop le quanh TAM GRID: khong gan ERA5
    # HANOI cho cam bien nam xa ngoai vung co du lieu khi tuong.
    # Day la guard bao thu dua vao geometry, KHONG phai nguong km tu dat.
    grid_name = F.col("grid_id")
    wx = (
        wx.withColumn("_dlat", F.split(grid_name, ":").getItem(2).cast("double"))
        .withColumn("_dlon", F.split(grid_name, ":").getItem(3).cast("double"))
    )
    bad_resolution = (
        ~F.col("grid_id").rlike(r"^era5:regular_ll:[0-9.]+:[0-9.]+:.*$")
        | F.col("_dlat").isNull() | F.col("_dlon").isNull()
        | F.isnan("_dlat") | F.isnan("_dlon")
        | ~F.col("_dlat").between(0.000001, 5.0)
        | ~F.col("_dlon").between(0.000001, 5.0)
    )
    if wx.filter(bad_resolution).limit(1).count():
        raise ValueError("Unsupported ERA5 grid_id or grid resolution")

    # Ghep chi tai DUNG gio t cua OpenAQ. LEFT JOIN giu cac dong AQ
    # ngay ca khi ngay do khong co file ERA5.
    # Delta longitude chuan hoa [-180,180] de tranh loi gan kinh tuyen 180.
    lon_delta = (
        F.pmod(F.col("a.longitude") - F.col("w.longitude") + F.lit(180.0),
               F.lit(360.0)) - F.lit(180.0)
    )
    spatial_coverage = (
        (F.abs(F.col("a.latitude") - F.col("w.latitude"))
         <= F.col("w._dlat") / F.lit(2.0) + F.lit(1e-8))
        & (F.abs(lon_delta)
           <= F.col("w._dlon") / F.lit(2.0) + F.lit(1e-8))
    )
    matching_time = F.col("a.timestamp_utc") == F.col("w.timestamp_utc")

    # Mot AQ hour co the co nhieu candidate grid. Chon khoang cach nho nhat,
    # neu bang nhau dung grid_id de luon ra cung 1 ket qua.
    candidates = (
        aq.alias("a")
        .join(wx.alias("w"), matching_time & spatial_coverage, "left")
        .withColumn(
            "_distance_km",
            haversine_km(
                F.col("a.latitude"), F.col("a.longitude"),
                F.col("w.latitude"), F.col("w.longitude"),
            ),
        )
    )
    ranking = Window.partitionBy(
        F.col("a.sensor_id"), F.col("a.parameter"), F.col("a.timestamp_utc")
    ).orderBy(F.col("_distance_km").asc_nulls_last(),
              F.col("w.grid_id").asc_nulls_last())

    nearest = candidates.withColumn("_rank", F.row_number().over(ranking))\
        .filter(F.col("_rank") == 1)

    # Weather timestamp nay LA moc cua nhiet do/gio (t).
    # Tuyet doi khong lay w.precipitation_mm_1h tai t cho AQ [t,t+1h)!
    with_instant_weather = nearest.select(
        *[F.col("a." + c).alias(c) for c in AQ_COLS],
        F.col("w.grid_id").alias("weather_grid_id"),
        F.col("w.timestamp_utc").alias("weather_timestamp_utc"),
        F.col("_distance_km").cast("double").alias("weather_distance_km"),
        F.col("w.temperature_c").alias("temperature_c"),
        F.col("w.wind_u_ms").alias("wind_u_ms"),
        F.col("w.wind_v_ms").alias("wind_v_ms"),
    )

    # Total precipitation trong ERA5 Silver la MUA 1 GIO KET THUC tai
    # validity_timestamp. De tinh mua cua AQ [t,t+1h), can ERA5 tai t+1h.
    # Chi ghep khi period_end_utc cua AQ thuc su bang t+1h; neu khong
    # dung 1 gio thi de precipitation=NULL, tranh gan sai khoang do.
    expected_end = F.expr("timestamp_utc + INTERVAL 1 HOUR")
    with_end = with_instant_weather.withColumn(
        "_rain_end_utc",
        F.when(F.col("period_end_utc") == expected_end, expected_end)
         .otherwise(F.lit(None).cast("timestamp")),
    )

    rain = wx.select(
        F.col("grid_id").alias("_rain_grid_id"),
        F.col("timestamp_utc").alias("_rain_validity_end"),
        F.col("precipitation_mm_1h").alias("_rain_mm"),
    )
    context = (
        with_end.join(
            rain,
            (F.col("weather_grid_id") == F.col("_rain_grid_id"))
            & (F.col("_rain_end_utc") == F.col("_rain_validity_end")),
            "left",
        )
        .withColumn(
            "precipitation_mm_1h", F.col("_rain_mm").cast("double")
        )
    )

    # Dung chinh thu tu cot trong contract, khong tu them cot staging.
    contract = json.loads(
        (root / "contracts/schemas/table_contracts.json").read_text(encoding="utf-8")
    )
    declared_cols = list(contract["tables"]["air_quality_context_hourly"]["fields"])
    result = context.select(*declared_cols)
    validate_contract(result, root)

    # Kiem tra tinh bat bien so dong va unique key sau left join.
    aq_count = aq.count()
    result_count = result.count()
    if result_count != aq_count:
        raise ValueError(f"LEFT JOIN changed AQ row count: {aq_count}->{result_count}")
    check_unique(result, ["sensor_id", "parameter", "timestamp_utc"], "AQ context")

    matched = result.filter(F.col("weather_grid_id").isNotNull()).count()
    rain_matched = result.filter(F.col("precipitation_mm_1h").isNotNull()).count()

    # Khong ghi de output co san (errorifexists), tranh lam mat du lieu.
    result.write.mode("errorifexists").parquet(output_path.as_posix())
    return {
        "air_quality_rows": aq_count,
        "context_rows": result_count,
        "weather_matched_rows": matched,
        "weather_missing_rows": result_count - matched,
        "precipitation_matched_rows": rain_matched,
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
        .appName("Air-Quality-Weather-Context-Curated")
        .master(config["processing"]["spark_master"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", config["processing"]["spark_driver_memory"])
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("ERROR")
        stats = build_context(spark, root, config)
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("Air quality context Curated completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
