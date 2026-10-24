
import json

# Bước 1: Đọc file PM2.5
with open("fixtures/curated/air_quality_context_hourly.json",
          "r", encoding="utf-8") as f:
    observations = json.load(f)

# Bước 2: Đọc file baseline
with open("fixtures/curated/air_quality_baseline.json",
          "r", encoding="utf-8") as f:
    baselines = json.load(f)

# Bước 3: Ghép hai bản ghi có cùng sensor,
# cùng parameter và cùng thời gian
for obs in observations:
    for base in baselines:

        same_sensor = obs["sensor_id"] == base["sensor_id"]
        same_parameter = obs["parameter"] == base["parameter"]
        same_time = obs["timestamp_utc"] == base["timestamp_utc"]

        if same_sensor and same_parameter and same_time:
            pm25 = obs["value_ugm3"]
            baseline = base["baseline_ugm3"]

            # Kiểm tra chất lượng dữ liệu
            valid = (
                pm25 is not None
                and baseline is not None
                and obs["has_flags"] is False
                and base["is_usable"] is True
            )

            if not valid:
                print("Dữ liệu không đủ điều kiện")
                continue

            # Kiểm tra hai ngưỡng
            condition_1 = pm25 - baseline >= 25
            condition_2 = pm25 >= 1.5 * baseline

            is_candidate = condition_1 and condition_2

            print("Sensor:", obs["sensor_id"])
            print("Thời gian:", obs["timestamp_utc"])
            print("PM2.5:", pm25)
            print("Baseline:", baseline)
            print("Candidate:", is_candidate)
