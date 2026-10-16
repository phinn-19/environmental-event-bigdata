# Sprint 05 — OpenAQ historical Raw coverage

**Phạm vi:** Ingestion / kiểm định Raw, không phải Spark Processing, không viết Silver/Curated Parquet, không tính baseline hoặc phát hiện sự kiện.

## Cài patch

Từ thư mục gốc repository trên Windows PowerShell, bảo đảm đang ở `feat/ingestion-coverage` và working tree chưa có thay đổi cần giữ. Trước khi giải nén gói, sao lưu bản `src/ingestion/coverage.py` đang chạy tốt vào `data/backups/`.

```powershell
New-Item -ItemType Directory -Force .\data\backups | Out-Null
Copy-Item .\src\ingestion\coverage.py .\data\backups\coverage_before_export.py
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\coverage_sprint05_patch.zip" -DestinationPath . -Force
```

**Cấu trúc patch:** `src/ingestion/coverage.py` (full replacement), `tests/test_ingestion_coverage_export.py` (bổ sung 5 tests, giữ nguyên 6 tests cũ), `docs/INGESTION_COVERAGE_SPRINT05.md` (tài liệu này).

## Chạy

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m src.ingestion.coverage --sensor-id 21632 --start 2024-10-02 --end 2024-11-01 --export
```

Các file báo cáo nằm trong `data/reports/ingestion/openaq/` (được `.gitignore` loại khỏi commit):

- `openaq_sensor_21632_2024-10-02_2024-11-01.json`: tổng quan, cảnh báo, số mẫu theo từng giờ UTC, chi tiết từng ngày.
- `openaq_sensor_21632_2024-10-02_2024-11-01_daily.csv`: mỗi ngày một hàng, mở được bằng Excel.

Ngưỡng `min_valid_samples` lấy từ `configs/project.yaml` thay vì ghi cứng.

## Phân biệt hai kết quả

- **Technical integrity:** manifest, Raw file, SHA-256, row count, ngày/giờ, thiếu/trùng giờ; vi phạm sẽ trả exit code khác 0. Dữ liệu nhiều trang/ngày được ghép trước khi xác minh đủ 24 giờ.
- **Preliminary quality warning:** thiếu giá trị (`null`), `hasFlags=true`, hoặc không có dòng usable; chỉ cảnh báo, không xóa bản ghi Raw và không tự biến Ingestion thành `failed`.

**Định nghĩa usable sơ bộ:** `value is not None` và `flagInfo.hasFlags is not True`. Chưa xác minh toàn bộ đơn vị, biên độ, chính sách QC chuyên sâu hoặc tính liên tục nhiều sensor. Kết quả theo giờ dùng làm bằng chứng về *số mẫu ứng viên*, **không phải** baseline đã được tính.

## Bộ kiểm thử bổ sung

- Hai page cùng ngày phải được ghép đủ 24 giờ.
- Thay đổi nội dung Raw phải làm checksum mismatch và không được xác nhận ngày thành công.
- Thiếu một giờ là technical problem.
- Một ngày có 24 bản ghi `null` + flagged vẫn được kiểm định file thành công, đồng thời nhận quality warning.
- Đọc `min_valid_samples` từ YAML.

Để PR, chạy toàn bộ test và đối chiếu báo cáo với kết quả cũ: 30 ngày, 720 bản ghi, 644 usable sơ bộ, 68 null, 76 flagged, 9 ngày cảnh báo, 24/24 giờ UTC đạt 10 mẫu. Những số liệu này chỉ thuộc sensor `21632`, khoảng 02–31/10/2024.
