
from datetime import datetime, timedelta

# BUOI 2.6 - GOM EVENT THEO THOI GIAN UTC
# Du lieu gia dinh, chua phai du lieu thuc te.

# 1. Du lieu cua hai sensor A va B
data = [
    {"sensor_id": "A", "timestamp_utc": "2024-11-01T13:00:00Z", "pm25": 100},
    {"sensor_id": "B", "timestamp_utc": "2024-11-01T13:00:00Z", "pm25": 90},
    {"sensor_id": "A", "timestamp_utc": "2024-11-01T08:00:00Z", "pm25": 80},
    {"sensor_id": "A", "timestamp_utc": "2024-11-01T09:00:00Z", "pm25": 90},
    {"sensor_id": "B", "timestamp_utc": "2024-11-01T11:00:00Z", "pm25": 80},
    {"sensor_id": "A", "timestamp_utc": "2024-11-01T11:00:00Z", "pm25": 85},
    {"sensor_id": "A", "timestamp_utc": "2024-11-01T12:00:00Z", "pm25": 95}
]

# Gia su baseline cua moi ban ghi la 30
baseline = 30


# 2. Ham chuyen chuoi UTC thanh datetime
def parse_time(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


# 3. Sap xep theo sensor va thoi gian
data.sort(
    key=lambda record: (
        record["sensor_id"],
        parse_time(record["timestamp_utc"])
    )
)

# 4. Chuan bi danh sach luu event
events = []
current_group = []
previous_record = None

# 5. Kiem tra tung ban ghi
for record in data:

    pm25 = record["pm25"]
    current_time = parse_time(record["timestamp_utc"])

    # Kiem tra hai nguong bat thuong
    condition_1 = pm25 - baseline >= 25
    condition_2 = pm25 >= 1.5 * baseline

    is_candidate = condition_1 and condition_2

    # Kiem tra voi ban ghi truoc do
    if previous_record is not None:

        same_sensor = (
            record["sensor_id"] == previous_record["sensor_id"]
        )

        previous_time = parse_time(
            previous_record["timestamp_utc"]
        )

        consecutive = (
            current_time - previous_time == timedelta(hours=1)
        )

        # Khac sensor hoac khong lien tiep: ngat nhom
        if not same_sensor or not consecutive:

            if len(current_group) >= 2:
                events.append(current_group.copy())

            current_group = []

    # Gom cac candidate
    if is_candidate:
        current_group.append(record)

    else:
        if len(current_group) >= 2:
            events.append(current_group.copy())

        current_group = []

    print(
        record["sensor_id"],
        record["timestamp_utc"],
        "Candidate:",
        is_candidate
    )

    previous_record = record

# 6. Luu nhom cuoi neu du dieu kien
if len(current_group) >= 2:
    events.append(current_group.copy())

# 7. Hien thi ket qua
print("\nTong so Event:", len(events))

for i, event in enumerate(events, start=1):

    values = [record["pm25"] for record in event]

    peak = max(values)
    mean = sum(values) / len(values)

    start_time = parse_time(event[0]["timestamp_utc"])
    end_time = parse_time(event[-1]["timestamp_utc"]) + timedelta(hours=1)

    duration = (end_time - start_time).total_seconds() / 3600

    print("--------------------")
    print("Event:", i)
    print("Sensor:", event[0]["sensor_id"])
    print("Bat dau:", start_time.isoformat())
    print("Ket thuc:", end_time.isoformat())
    print("Peak:", peak)
    print("Mean:", round(mean, 2))
    print("Thoi luong:", duration, "gio")
