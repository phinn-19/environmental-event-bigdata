
import json
import math
from pathlib import Path
from datetime import datetime, timedelta

# BUOI 2.7 - DOC FIXTURES VA PHAT HIEN AQ EVENT

# 1. Tim thu muc goc du an
ROOT = Path(__file__).resolve().parents[1]

# 2. Doc hai file JSON
obs_path = ROOT / "fixtures/curated/air_quality_context_hourly.json"
base_path = ROOT / "fixtures/curated/air_quality_baseline.json"

with obs_path.open("r", encoding="utf-8") as f:
    observations = json.load(f)

with base_path.open("r", encoding="utf-8") as f:
    baselines = json.load(f)


# 3. Ham ho tro
def make_key(record):
    return (
        record["sensor_id"],
        record["parameter"],
        record["timestamp_utc"]
    )


def parse_time(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def valid_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


# 4. Tao bang tra cuu baseline
baseline_index = {}

for base in baselines:
    key = make_key(base)

    if key in baseline_index:
        raise ValueError("Baseline bi trung: " + str(key))

    baseline_index[key] = base


# 5. Sap xep du lieu theo sensor va thoi gian
observations.sort(
    key=lambda x: (
        x["sensor_id"],
        x["parameter"],
        x["timestamp_utc"]
    )
)

candidates = []
seen_keys = set()

# 6. Kiem tra tung observation
for obs in observations:
    key = make_key(obs)

    if key in seen_keys:
        raise ValueError("Observation bi trung: " + str(key))

    seen_keys.add(key)

    if obs["parameter"] != "pm25":
        continue

    base = baseline_index.get(key)
    pm25 = obs["value_ugm3"]

    problems = []

    # Kiem tra chat luong observation
    if obs["has_flags"] is not False:
        problems.append("Du lieu bi flag")

    if not valid_number(pm25):
        problems.append("PM2.5 thieu hoac khong hop le")

    # Kiem tra baseline
    if base is None:
        problems.append("Khong tim thay baseline")

    elif base["is_usable"] is not True:
        problems.append("Baseline khong usable")

    elif not valid_number(base["baseline_ugm3"]):
        problems.append("Gia tri baseline khong hop le")

    print("\nSensor:", obs["sensor_id"])
    print("Thoi gian:", obs["timestamp_utc"])

    if problems:
        print("Bo qua:", ", ".join(problems))
        continue

    baseline = base["baseline_ugm3"]

    # Kiem tra hai nguong
    condition_1 = pm25 - baseline >= 25
    condition_2 = pm25 >= 1.5 * baseline

    is_candidate = condition_1 and condition_2

    print("PM2.5:", pm25)
    print("Baseline:", baseline)
    print("Candidate:", is_candidate)

    if is_candidate:
        candidates.append(obs)


# 7. Gom candidate lien tiep theo sensor
events = []
current_group = []

for record in candidates:

    if current_group:
        previous = current_group[-1]

        same_sensor = (
            record["sensor_id"] == previous["sensor_id"]
        )

        consecutive = (
            parse_time(record["timestamp_utc"])
            - parse_time(previous["timestamp_utc"])
            == timedelta(hours=1)
        )

        if not same_sensor or not consecutive:
            if len(current_group) >= 2:
                events.append(current_group)

            current_group = []

    current_group.append(record)

# Xu ly nhom cuoi cung
if len(current_group) >= 2:
    events.append(current_group)


# 8. In ket qua
print("\n========================")
print("So candidate:", len(candidates))
print("Tong so Event:", len(events))

for i, event in enumerate(events, start=1):
    values = [row["value_ugm3"] for row in event]

    print("--------------------")
    print("Event:", i)
    print("Sensor:", event[0]["sensor_id"])
    print("Bat dau:", event[0]["timestamp_utc"])
    print("Peak:", max(values))
    print("Mean:", round(sum(values) / len(values), 2))
    print("So gio:", len(event))
