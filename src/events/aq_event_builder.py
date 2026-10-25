
"""Build contract-compliant AQ events from consecutive candidate groups."""

import math
from datetime import timedelta
from statistics import median

from src.common.validation import timestamp, validate_events


# 1. Kiem tra PM2.5 va baseline co hop le khong
def _valid_measurement(value):
    """Accept only non-negative, finite numbers (not bool or null)."""
    return (
        type(value) in (int, float)
        and math.isfinite(value)
        and value >= 0
    )


# 2. Bien cac nhom candidate thanh Event
def build_aq_events(groups, region_id, algorithm_version="aq-v1"):
    """Convert groups from group_consecutive_candidates() to AQ event dicts."""

    if not isinstance(region_id, str) or not region_id.strip():
        raise ValueError("region_id must be a non-empty string")

    if not isinstance(algorithm_version, str) or not algorithm_version.strip():
        raise ValueError("algorithm_version must be a non-empty string")

    events = []

    # 3. Xu ly lan luot tung nhom
    for group in groups:

        if len(group) < 2:
            raise ValueError("Each AQ event needs at least 2 candidate hours")

        # Sap xep theo thoi gian thuc
        ordered = sorted(
            group,
            key=lambda item: timestamp(
                item["observation"]["timestamp_utc"]
            ),
        )

        first = ordered[0]["observation"]
        sensor_id = first["sensor_id"]

        if first["parameter"] != "pm25":
            raise ValueError("AQ event parameter must be pm25")

        values = []
        baselines = []
        evidence_ids = []
        previous_end = None

        # 4. Kiem tra va thu thap thong tin moi gio
        for item in ordered:
            obs = item["observation"]

            if (
                obs["sensor_id"] != sensor_id
                or obs["parameter"] != "pm25"
            ):
                raise ValueError(
                    "All candidate hours must use the same sensor and pm25"
                )

            start = timestamp(obs["timestamp_utc"])
            end = timestamp(obs["period_end_utc"])

            if end - start != timedelta(hours=1):
                raise ValueError(
                    "Each observation must cover exactly one hour"
                )

            if previous_end is not None and start != previous_end:
                raise ValueError(
                    "AQ candidate hours must be consecutive"
                )

            previous_end = end

            value = obs["value_ugm3"]
            baseline = item["baseline_ugm3"]

            if (
                not _valid_measurement(value)
                or not _valid_measurement(baseline)
            ):
                raise ValueError(
                    "PM2.5 and baseline must be valid non-negative numbers"
                )

            values.append(float(value))
            baselines.append(float(baseline))

            evidence_ids.append(
                f"aq:{sensor_id}:pm25:{obs['timestamp_utc']}"
            )

        # 5. Tinh thoi gian va metrics cua Event
        start_time = first["timestamp_utc"]
        end_time = ordered[-1]["observation"]["period_end_utc"]

        duration_hours = (
            timestamp(end_time) - timestamp(start_time)
        ).total_seconds() / 3600

        # 6. Tao Event theo Data Contract
        event = {
            "event_id": (
                f"{algorithm_version}:{region_id}:{sensor_id}:{start_time}"
            ),
            "event_type": "AIR_QUALITY_ANOMALY",
            "region_id": region_id,
            "start_time_utc": start_time,
            "end_time_utc": end_time,
            "centroid_latitude": first["latitude"],
            "centroid_longitude": first["longitude"],
            "severity": "undetermined",
            "metrics": {
                "sensor_ids": [sensor_id],
                "parameter": "pm25",
                "peak_ugm3": max(values),
                "mean_ugm3": sum(values) / len(values),
                "baseline_ugm3": float(median(baselines)),
                "duration_hours": duration_hours,
            },
            "weather_context": None,
            "evidence_ids": evidence_ids,
            "algorithm_version": algorithm_version,
        }

        events.append(event)

    # 7. Kiem tra toan bo Event truoc khi tra ket qua
    validate_events(events)

    return events
