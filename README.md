# Environmental Event Big Data — Foundation v1.1

**Đề tài:** Xây dựng hệ thống thu thập, tích hợp và phân tích sự kiện môi trường từ dữ liệu quan trắc, khí tượng và viễn thám tại Việt Nam.

**Trạng thái:** FOUNDATION ONLY / **đề xuất chờ nhóm 3 người duyệt**. Chưa có ETL thực tế, Spark processing, event detection và dashboard. Ba nguồn đã được kiểm thử truy xuất ngoài repo này; một số sample tồn tại trên máy người tạo, không có nghĩa ở repo công khai.

## Quick start — Windows PowerShell (Python 3.12.x)

```powershell
cd C:\Users\Admin\environmental-event-bigdata
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe main.py doctor
.\.venv\Scripts\python.exe main.py mock-run
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Khi đồng đội clone về mà **chưa có** `.venv`:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe main.py doctor
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

`mock-run` CHỈ sao chép và kiểm tra **SYNTHETIC EVENT FIXTURES** sang `data/demo/events_mock.json`; không dùng làm anomaly thật hay benchmark. Lệnh `python main.py run` cố ý trả exit code khác 0 cho đến khi ba module được phát triển và tích hợp.

## Data flow & ownership

| Phụ trách | Code | Input | Output contract |
|---|---|---|---|
| Người 1 | `src/ingestion/` | 3 API + `configs/project.yaml` | `data/raw/**` + manifest |
| Người 2 | `src/processing/` | raw, fixtures | `data/silver/{table}/`, `data/curated/{table}/` **Parquet** |
| Người 3 | `src/events/`, `src/dashboard/` | curated Parquet / mock fixtures | `data/events/events.json`, dashboard |
| Cả nhóm | `src/common/`, `configs/`, `contracts/`, `main.py` | PR/review | compatibility / E2E |

**Không tự ý đổi** tên bảng/cột, UTC, format hay units. `contracts/` mới là nguồn sự thật cho schema; `contracts/schemas/` là bản máy đọc để kiểm thử JSON fixture. Khi tài liệu Markdown và schema máy đọc có khác biệt, dừng merge và sửa cả hai trong cùng PR.

## Repo contents

- `configs/project.yaml`: region/config mặc định, **không** secrets, integration window để null tới khi xác minh dữ liệu giao nhau.
- `contracts/*.md`: Project / Data / Acquisition contracts v1 + **Contract Decisions v1.1** (proposed for team approval).
- `contracts/schemas/`: machine-readable JSON schemas cho dữ liệu mẫu/events và manifest.
- `fixtures/silver/`, `fixtures/curated/`, `fixtures/events/`, `fixtures/scenarios/`: **SYNTHETIC TEST ONLY**. Đây là JSON fixtures, không phải đầu ra Parquet thật.
- `src/`: interface CLI placeholder; các lệnh module chưa triển khai trả mã lỗi `3` và nói rõ.
- `tests/`: config + fixture contract smoke tests, including v1.1 scenario examples; **not** production algorithm tests. Chạy không cần Spark hay API keys.
- `data/`: máy local, không commit.
- `scripts/`: giữ nguyên script kiểm chứng API trước đây. Sau này có thể chuyển sang `experiments/` qua PR.

## Phạm vi MVP đã đề xuất

- ERA5: `2m_temperature`, `2m_dewpoint_temperature`, `10m_u_component_of_wind`, `10m_v_component_of_wind`, `total_precipitation`.
- OpenAQ: PM2.5 hourly + sensor/location metadata và flags.
- NASA FIRMS: NOAA-20 SP cho lịch sử nếu khả dụng, NRT cho dữ liệu mới; raw CSV 14 cột.
- Spark local, batch-first, Parquet, UTC, WGS84. Java/PySpark versions **chưa pin** đến khi test compatibility thực tế.
- `AIR_QUALITY_ANOMALY` và `FIRE_HOTSPOT_CLUSTER`; LLM/streaming optional.

## Chốt trước khi 3 người tách nhánh

1. Cả ba duyệt v1 contracts, region, event JSON, dependency policy.
2. Chấp nhận synthetic mock data chỉ để phát triển và kiểm thử, không báo cáo là dữ liệu thực.
3. Duyệt `docs/TEAM_WORKFLOW.md`; review và merge sớm vào `develop`.
4. Giao ownership cho các vấn đề mở: giai đoạn 3 nguồn giao nhau; OpenAQ continuity, FIRMS SP archive; PM2.5 QC; Spark/Java version.

**Bảo mật:** không commit `.cdsapirc`, MAP_KEY, OpenAQ key, `.env`, `.venv/`, downloaded datasets. `.env.example` chỉ là tài liệu tham khảo và không tự load secrets. Các sample provider có thể có điều kiện sử dụng riêng; không tự copy từ `data/` sang `fixtures/` trước khi kiểm tra quyền sử dụng.

## Contract Decisions v1.1 (proposed)

`configs/project.yaml` now contains reference thresholds and names for baseline / AQ anomaly / fire clustering. These are **development defaults**, not scientifically validated decisions. Ownership: Member 3 specifies algorithm semantics, Member 2 implements the baseline on Spark; ERA5 is context only. Group approves in PR before assigning three feature branches.
