
# BUỔI 2.5 - GOM CANDIDATE THÀNH EVENT
# VÀ TÍNH PEAK, MEAN, DURATION

# 1. Dữ liệu PM2.5 giả định của cùng một sensor
data = [
    {"hour": 8, "pm25": 80},
    {"hour": 9, "pm25": 90},
    {"hour": 10, "pm25": 40},
    {"hour": 11, "pm25": 85},
    {"hour": 12, "pm25": 95},
    {"hour": 13, "pm25": 100},
    {"hour": 14, "pm25": 35}
]

# 2. Mức nền tham chiếu
baseline = 30

# 3. Danh sách lưu các event
events = []

# Nhóm candidate liên tiếp đang xét
current_group = []

# 4. Kiểm tra từng giờ
for record in data:

    pm25 = record["pm25"]

    # Điều kiện 1: Tăng ít nhất 25
    condition_1 = pm25 - baseline >= 25

    # Điều kiện 2: Ít nhất 1.5 lần baseline
    condition_2 = pm25 >= 1.5 * baseline

    # Cả hai điều kiện phải đúng
    is_candidate = condition_1 and condition_2

    print(record["hour"], "h:", is_candidate)

    # 5. Gom các candidate
    if is_candidate:
        current_group.append(record)

    else:
        # Chỉ tạo event nếu có ít nhất 2 giờ
        if len(current_group) >= 2:
            events.append(current_group)

        # Bắt đầu nhóm mới
        current_group = []

# 6. Kiểm tra nhóm cuối cùng
if len(current_group) >= 2:
    events.append(current_group)

# 7. Hiển thị số lượng event
print("\nTổng số Event:", len(events))

# 8. Tính thông tin từng event
for i, event in enumerate(events, start=1):

    values = []

    # Lấy PM2.5 của từng giờ trong event
    for record in event:
        values.append(record["pm25"])

    # Tính các chỉ số
    peak = max(values)
    mean = sum(values) / len(values)
    duration = len(event)

    # Thời gian bắt đầu và kết thúc
    start_hour = event[0]["hour"]
    end_hour = event[-1]["hour"] + 1

    # Hiển thị kết quả
    print("--------------------")
    print("Event:", i)
    print("Bắt đầu:", start_hour, "h")
    print("Kết thúc:", end_hour, "h")
    print("PM2.5 cao nhất:", peak)
    print("PM2.5 trung bình:", round(mean, 2))
    print("Thời lượng:", duration, "giờ")
