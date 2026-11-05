"""Publish only target UTC day rows from six completed Spark tables.

The AQ Silver input deliberately contains <=30 days' history to calculate
baseline. This script runs AFTER the six modules and slices *each output*
back to its own target day, preserving computed baseline/context values.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil


def day(text: str) -> datetime:
    value = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if value.strftime("%Y-%m-%d") != text:
        raise ValueError(f"Invalid UTC day: {text}")
    return value


def table_days(selection: dict) -> dict[str, str]:
    mapping = {
        "air_quality_hourly": "aq_day",
        "air_quality_baseline": "aq_day",
        "air_quality_context_hourly": "aq_day",
        "fire_hotspots": "firms_day",
        "fire_hotspots_context": "firms_day",
        "weather_hourly": "era5_day",
    }
    # Validates the day format and enforces that it's an actual calendar day.
    return {table: day(selection[key]).strftime("%Y-%m-%d") for table, key in mapping.items()}


def scope(job: Path, dest: Path) -> dict:
    import yaml
    from pyspark.sql import SparkSession, functions as F

    job, dest = job.resolve(), dest.resolve()
    if not job.is_dir() or not (job / "SELECTION.json").is_file():
        raise ValueError("Missing complete Processing job / SELECTION.json")
    if dest.exists() or dest == job or dest.is_relative_to(job) or job.is_relative_to(dest):
        raise ValueError("Refusing existing or overlapping scoped output directory")
    selection = json.loads((job / "SELECTION.json").read_text(encoding="utf-8"))
    days = table_days(selection)
    config = yaml.safe_load((job / "configs/project.yaml").read_text(encoding="utf-8"))
    if config["project"]["timezone"] != "UTC" or config["processing"]["timezone"] != "UTC":
        raise ValueError("UTC required")
    if config["project"]["data_root"] != "data":
        raise ValueError("Scoped packager expects project.data_root=data")
    spark = (SparkSession.builder.appName("Processing-Bounded-Publication-Scope")
             .master(config["processing"]["spark_master"])
             .config("spark.driver.memory", config["processing"]["spark_driver_memory"])
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    report: dict = {"days_utc": days, "tables": {}, "scope": "UTC half-open [00:00, next 00:00)",
                    "qc_warnings": []}
    try:
        spark.sparkContext.setLogLevel("ERROR")
        dest.mkdir(parents=True)
        for tier, names in {
            "silver": ("air_quality_hourly", "fire_hotspots", "weather_hourly"),
            "curated": ("air_quality_baseline", "air_quality_context_hourly", "fire_hotspots_context"),
        }.items():
            for table in names:
                source_path = job / "data" / tier / table
                if not (source_path / "_SUCCESS").is_file():
                    raise FileNotFoundError(f"Missing successful Spark output {tier}/{table}")
                df = spark.read.parquet(str(source_path))
                if "timestamp_utc" not in df.columns or not list(source_path.glob("*.parquet")):
                    raise ValueError(f"Missing timestamp/Parquet for {tier}/{table}")
                start = day(days[table])
                end = start + timedelta(days=1)
                lo = F.to_timestamp(F.lit(start.strftime("%Y-%m-%d %H:%M:%S")), "yyyy-MM-dd HH:mm:ss")
                hi = F.to_timestamp(F.lit(end.strftime("%Y-%m-%d %H:%M:%S")), "yyyy-MM-dd HH:mm:ss")
                sliced = df.filter((F.col("timestamp_utc") >= lo) & (F.col("timestamp_utc") < hi))
                output_path = dest / "data" / tier / table
                output_path.parent.mkdir(parents=True, exist_ok=True)
                count = sliced.count()
                sliced.write.mode("errorifexists").parquet(str(output_path))
                if not (output_path / "_SUCCESS").is_file():
                    raise ValueError(f"Write incomplete {tier}/{table}")
                item = {"day_utc": days[table], "rows": count}
                if table == "air_quality_baseline":
                    item["usable_rows"] = sliced.filter(F.col("is_usable") == True).count()
                if table in ("air_quality_context_hourly", "fire_hotspots_context"):
                    item["weather_matched_rows"] = sliced.filter(F.col("weather_grid_id").isNotNull()).count()
                report["tables"][table] = item
                if count == 0:
                    report["qc_warnings"].append(f"EMPTY_TARGET_DAY: {table}")
                print(f"SCOPE {tier}/{table}: {count} rows UTC {days[table]}")
                if table == "weather_hourly" and (source_path / "_era5_quality.json").is_file():
                    shutil.copy2(source_path / "_era5_quality.json", output_path / "_era5_input_quality.json")
        for silver, curated in (
            ("air_quality_hourly", "air_quality_baseline"),
            ("air_quality_hourly", "air_quality_context_hourly"),
            ("fire_hotspots", "fire_hotspots_context"),
        ):
            if report["tables"][silver]["rows"] != report["tables"][curated]["rows"]:
                raise ValueError(f"Target-day row count mismatch: {silver} vs {curated}")
        # Distinguish absent 23:00 rain context due to missing t+1h ERA5.
        report["qc_warnings"].append("ERA5 next-day 00 UTC is not selected by this daily workflow; last-hour rain context may be null")
        shutil.copy2(job / "SELECTION.json", dest / "SELECTION.json")
        (dest / "SCOPE_REPORT.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print("TARGET-DAY SCOPE: PASS; all six outputs and row-count invariants verified")
        return report
    except BaseException:
        if dest.exists():
            shutil.rmtree(dest)
        raise
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--job", type=Path, required=True)
    p.add_argument("--dest", type=Path, required=True)
    a = p.parse_args()
    scope(a.job, a.dest)


if __name__ == "__main__":
    main()
