
"""Detect PM2.5 candidates and group consecutive hours."""

import math
from datetime import timedelta

from src.common.validation import timestamp


# 1. Tao khoa de ghep observation va baseline
def _key(row):
    return (
        row["sensor_id"],
        row["parameter"],
        row["timestamp_utc"]
    )


# 2. Kiem tra gia tri so hop le
def _valid_number(value):
    return (
        type(value) in (int, float)
        and math.isfinite(value)
        and value >= 0
    )


# 3. Kiem tra mot ban ghi co la candidate khong
def is_aq_candidate(observation, baseline, config):

    if baseline is None or observation["parameter"] != "pm25":
        return False

    if _key(observation) != _key(baseline):
        return False

    rules = config["algorithms"]["air_quality"]
    baseline_rules = rules["baseline"]
    anomaly_rules = rules["anomaly"]

    # Kiem tra chat luong du lieu
    if observation["has_flags"] is not False:
        return False

    if baseline["is_usable"] is not True:
        return False

    if baseline["method"] != baseline_rules["method"]:
        return False

    if baseline["sample_count"] < baseline_rules["min_valid_samples"]:
        return False

    value = observation["value_ugm3"]
    reference = baseline["baseline_ugm3"]

    if not _valid_number(value) or not _valid_number(reference):
        return False

    # Moi observation phai dai dung 1 gio
    duration = (
        timestamp(observation["period_end_utc"])
        - timestamp(observation["timestamp_utc"])
    )

    if duration != timedelta(hours=1):
        return False

    # Hai dieu kien bat thuong
    condition_1 = (
        value - reference
        >= anomaly_rules["min_absolute_increase_ugm3"]
    )

    condition_2 = (
        value >= anomaly_rules["min_ratio"] * reference
    )

    return condition_1 and condition_2


# 4. Phat hien tat ca candidate
def detect_aq_candidates(observations, baselines, config):

    baseline_index = {}

    # Tao bang tra cuu baseline
    for baseline in baselines:
        key = _key(baseline)

        if key in baseline_index:
            raise ValueError(f"Duplicate baseline key: {key}")

        baseline_index[key] = baseline

    candidates = []
    seen = set()

    for observation in observations:
        key = _key(observation)

        if key in seen:
            raise ValueError(f"Duplicate observation key: {key}")

        seen.add(key)

        baseline = baseline_index.get(key)

        if is_aq_candidate(observation, baseline, config):
            candidates.append({
                "observation": observation,
                "baseline_ugm3": baseline["baseline_ugm3"],
            })

    # Sap xep theo sensor, parameter, thoi gian
    candidates.sort(
        key=lambda item: (
            item["observation"]["sensor_id"],
            item["observation"]["parameter"],
            item["observation"]["timestamp_utc"]
        )
    )

    return candidates


# 5. GOM CANDIDATE THANH CAC NHOM EVENT
def group_consecutive_candidates(candidates, config):

    # Lay so gio lien tiep toi thieu tu config
    min_hours = config["algorithms"]["air_quality"]["anomaly"][
        "min_consecutive_hours"
    ]

    # Sap xep candidate de xu ly dung thu tu
    ordered = sorted(
        candidates,
        key=lambda item: (
            item["observation"]["sensor_id"],
            item["observation"]["parameter"],
            timestamp(item["observation"]["timestamp_utc"])
        )
    )

    groups = []
    current_group = []

    for item in ordered:
        obs = item["observation"]

        if current_group:
            previous = current_group[-1]["observation"]

            # Kiem tra cung sensor
            same_sensor = (
                obs["sensor_id"] == previous["sensor_id"]
            )

            # Kiem tra cung parameter
            same_parameter = (
                obs["parameter"] == previous["parameter"]
            )

            # Kiem tra cach nhau dung 1 gio
            one_hour_apart = (
                timestamp(obs["timestamp_utc"])
                - timestamp(previous["timestamp_utc"])
                == timedelta(hours=1)
            )

            # Khong thoa dieu kien lien tiep
            if not (
                same_sensor
                and same_parameter
                and one_hour_apart
            ):
                # Nhom cu du so gio thi luu
                if len(current_group) >= min_hours:
                    groups.append(current_group)

                # Bat dau nhom moi
                current_group = []

        # Them candidate vao nhom hien tai
        current_group.append(item)

    # Kiem tra nhom cuoi cung
    if len(current_group) >= min_hours:
        groups.append(current_group)

    return groups
