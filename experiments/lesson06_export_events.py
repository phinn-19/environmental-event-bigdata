
import json
import math
import sys

from pathlib import Path
from datetime import datetime, timedelta
from statistics import median

# BUOI 2.8 - TAO AQ EVENT JSON
# CHI SU DUNG DU LIEU SYNTHETIC DE HOC

# 1. Xac dinh thu muc goc
ROOT = Path(__file__).resolve().parents[1]

# Cho phep su dung module validation cua du an
sys.path.insert(0, str(ROOT))

from src.common.validation import validate_events


# 2. Ham doc JSON
def read_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# 3. Ham xu ly thoi gian UTC
def parse_time(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


# 4. Kiem tra gia tri so
def valid_number(value):
    return (
        type(value) in (int, float)
        and math.isfinite(value)
        and value >= 0
    )


# 5. Doc du lieu mau tu repository
observations = read_json(
    ROOT / "fixtures/curated/air_quality_context_hourly.json"
)

baselines = read_json(
    ROOT / "fixtures/curated/air_quality_baseline.json"
)


# 6. Tao them 1 observation SYNTHETIC luc 12h
obs11 = next(
    row for row in observations
    if row["sensor_id"] == "21632"
    and row["timestamp_utc"] == "2024-11-01T11:00:00Z"
)

obs12 = obs11.copy()

obs12["timestamp_utc"] = "2024-11-01T12:00:00Z"
obs12["period_end_utc"] = "2024-11-01T13:00:00Z"
obs12["value_ugm3"] = 70.0
obs12["has_flags"] = False
obs12["source"] = "synthetic"

# Khong tu sao chep thoi tiet cua gio 11
for field in (
    "weather_grid_id",
    "weather_timestamp_utc",
    "weather_distance_km",
    "temperature_c",
    "wind_u_ms",
    "wind_v_ms",
    "precipitation_mm_1h"
):
    obs12[field] = None

observations.append(obs12)


# 7. Tao baseline SYNTHETIC cho gio 12
base11 = next(
    row for row in baselines
    if row["sensor_id"] == "21632"
    and row["timestamp_utc"] == "2024-11-01T11:00:00Z"
)

base12 = base11.copy()
base12["timestamp_utc"] = "2024-11-01T12:00:00Z"

# Baseline 12h la gia dinh, KHONG duoc tinh tu lich su
baselines.append(base12)


# 8. Tao bang tra cuu baseline
def make_key(row):
    return (
        row["sensor_id"],
        row["parameter"],
        row["timestamp_utc"]
    )


baseline_index = {}

for base in baselines:
    key = make_key(base)

    if key in baseline_index:
        raise ValueError("Trung baseline: " + str(key))

    baseline_index[key] = base


# 9. Kiem tra tung observation
candidates = []
seen = set()

for obs in observations:
    key = make_key(obs)

    if key in seen:
        raise ValueError("Trung observation: " + str(key))

    seen.add(key)

    if obs["parameter"] != "pm25":
        continue

    base = baseline_index.get(key)
    pm25 = obs["value_ugm3"]

    valid = (
        valid_number(pm25)
        and obs["has_flags"] is False
        and parse_time(obs["period_end_utc"])
            - parse_time(obs["timestamp_utc"])
            == timedelta(hours=1)
        and base is not None
        and base["is_usable"] is True
        and base["sample_count"] >= 10
        and valid_number(base["baseline_ugm3"])
    )

    is_candidate = False

    if valid:
        baseline = base["baseline_ugm3"]

        condition_1 = pm25 - baseline >= 25
        condition_2 = pm25 >= 1.5 * baseline

        is_candidate = condition_1 and condition_2

    print(
        obs["timestamp_utc"],
        "Candidate:",
        is_candidate
    )

    if is_candidate:
        candidates.append({
            "observation": obs,
            "baseline": base["baseline_ugm3"]
        })


# 10. Sap xep candidate
candidates.sort(
    key=lambda x: (
        x["observation"]["sensor_id"],
        parse_time(x["observation"]["timestamp_utc"])
    )
)


# 11. Gom cac gio lien tiep
groups = []
current_group = []

for item in candidates:
    obs = item["observation"]

    if current_group:
        previous = current_group[-1]["observation"]

        same_sensor = (
            obs["sensor_id"] == previous["sensor_id"]
        )

        consecutive = (
            parse_time(obs["timestamp_utc"])
            - parse_time(previous["timestamp_utc"])
            == timedelta(hours=1)
        )

        if not same_sensor or not consecutive:
            if len(current_group) >= 2:
                groups.append(current_group)

            current_group = []

    current_group.append(item)

if len(current_group) >= 2:
    groups.append(current_group)


# 12. Tao Event JSON
events = []

for i, group in enumerate(groups, start=1):
    rows = [x["observation"] for x in group]
    values = [x["value_ugm3"] for x in rows]
    baseline_values = [x["baseline"] for x in group]

    start_time = rows[0]["timestamp_utc"]
    end_time = rows[-1]["period_end_utc"]

    duration = (
        parse_time(end_time) - parse_time(start_time)
    ).total_seconds() / 3600

    sensor_id = rows[0]["sensor_id"]

    evidence_ids = [
        f"aq:{sensor_id}:pm25:{row['timestamp_utc']}"
        for row in rows
    ]

    event = {
        "event_id": f"synthetic-aq-lesson06-{i:03d}",
        "event_type": "AIR_QUALITY_ANOMALY",
        "region_id": "HANOI",
        "start_time_utc": start_time,
        "end_time_utc": end_time,
        "centroid_latitude": rows[0]["latitude"],
        "centroid_longitude": rows[0]["longitude"],
        "severity": "undetermined",
        "metrics": {
            "sensor_ids": [sensor_id],
            "parameter": "pm25",
            "peak_ugm3": max(values),
            "mean_ugm3": round(
                sum(values) / len(values), 2
            ),
            "baseline_ugm3": float(
                median(baseline_values)
            ),
            "duration_hours": duration
        },
        "weather_context": None,
        "evidence_ids": evidence_ids,
        "algorithm_version": "aq-v1"
    }

    events.append(event)


# 13. Kiem tra event theo contract cua nhom
validated_count = validate_events(events)

# 14. Ghi JSON vao thu muc demo
output_path = ROOT / "data/demo/events_lesson06_synthetic.json"

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)

with output_path.open("w", encoding="utf-8") as f:
    json.dump(
        events,
        f,
        indent=2,
        ensure_ascii=False,
        allow_nan=False
    )

# 15. In ket qua
print("\n==========================")
print("So candidate:", len(candidates))
print("Tong so Event:", len(events))
print("So Event hop le:", validated_count)
print("File da tao:", output_path)

for event in events:
    print("--------------------------")
    print("Event ID:", event["event_id"])
    print("Bat dau:", event["start_time_utc"])
    print("Ket thuc:", event["end_time_utc"])
    print("Peak:", event["metrics"]["peak_ugm3"])
    print("Mean:", event["metrics"]["mean_ugm3"])
    print("Duration:", event["metrics"]["duration_hours"])
