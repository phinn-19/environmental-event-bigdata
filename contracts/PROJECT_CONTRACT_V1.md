# PROJECT CONTRACT v1.0 — Environmental Event Big Data

**Trạng thái:** Đề xuất chốt nội bộ (nhóm 3 người cần đọc và xác nhận)  
**Đề tài:** Xây dựng hệ thống thu thập, tích hợp và phân tích sự kiện môi trường từ dữ liệu quan trắc, khí tượng và viễn thám tại Việt Nam  
**Mục tiêu tài liệu:** Cho phép 3 thành viên phát triển song song, ghép được module mà không phụ thuộc việc người trước đã viết xong toàn bộ code.  
**Phiên bản hợp đồng:** `v1.0` — thay đổi breaking phải được cả nhóm duyệt.

## 1. Quyết định đã chốt cho MVP

| Nội dung | Quy ước v1 |
|---|---|
| Nguồn | ERA5 (khí tượng), NASA FIRMS (hotspot), OpenAQ (quan trắc) |
| Nghiệp vụ | Việt Nam; vùng tải thử có thể là bounding box lớn hơn biên giới Việt Nam |
| Sự kiện | `AIR_QUALITY_ANOMALY` (MVP ưu tiên PM2.5) và `FIRE_HOTSPOT_CLUSTER` |
| Ngôn ngữ/môi trường | Python **3.12.x**, `.venv` riêng trên máy mỗi người |
| Xử lý dữ liệu | Batch-first; **Spark local** dùng ở processing, chưa yêu cầu Kafka/cluster |
| Lưu trữ | Raw giữ **GRIB/CSV/JSON**; lớp chuẩn hóa lưu **Parquet**; event cung cấp **JSON** và có thể lưu Parquet |
| Quy chuẩn thời gian | Mọi timestamp canonical đều là **UTC**; khoảng truy vấn `[start, end)` (end exclusive) |
| Không gian | Latitude/longitude theo **WGS84 / EPSG:4326**, độ thập phân, không đảo thứ tự |
| Đơn vị | PM2.5/PM10: `µg/m³`; nhiệt độ: `°C`; gió: `m/s`; FRP: `MW` |
| Bí mật | API keys trong biến môi trường/`.cdsapirc`, **không commit** Git |
| Git | `main` ổn định; `develop` tích hợp; `feat/ingestion`, `feat/processing`, `feat/events-dashboard` là 3 nhánh phát triển |

**Không chốt cứng ở v1:** mô hình/threshold anomaly, tham số clustering, LLM, Kafka, DB server, giao diện cuối, tối ưu Spark, năm dữ liệu lịch sử chính. Những mục này phải qua thử nghiệm. Không tuyên bố streaming realtime nếu chỉ là batch/polling/replay.

## 2. Sự thật đã kiểm chứng và điều chưa kiểm chứng

- **OpenAQ:** Truy vấn được metadata và measurement PM2.5 Hà Nội (location `7441`, sensor `21632`); 24 giờ mẫu có **22 giá trị không null**, 2 giá trị null; cờ chất lượng vẫn phải được bảo tồn.
- **NASA FIRMS:** Đã truy vấn được 10 hotspot mẫu VIIRS NOAA-20 trong khu vực mở rộng; gồm `latitude`, `longitude`, `acq_date`, `acq_time`, `confidence`, `frp`...
- **ERA5:** Tải/đọc được mẫu GRIB ngày 15/01/2025: **24 giờ × 2.145 điểm lưới = 51.480 giá trị**, 105.552 bytes, biến 2m temperature.
- **Chưa kiểm chứng:** ba mẫu **không cùng thời gian** (OpenAQ 11/2024, ERA5 01/2025, FIRMS 10/2026). Trước khi tuyên bố tích hợp đa nguồn thật, cần chọn một **integration window** có dữ liệu phù hợp và kiểm tra tính liên tục/độ phủ. Không dùng dữ liệu giả làm kết quả nghiên cứu.

## 3. Ranh giới trách nhiệm giữa ba module

```text
Người 1: src/ingestion/     API → data/raw/ + manifest
                                    |
Người 2: src/processing/   raw → data/silver/ → data/curated/
                                    |
Người 3: src/events/       curated → data/events/events.json
          src/dashboard/    data/events/ → dashboard
```

**Người 1 — Ingestion:** tải đúng nguồn, hỗ trợ cấu hình region/window, ghi nguyên bản dữ liệu + metadata + lỗi; không tự sửa schema silver.  
**Người 2 — Processing:** giải mã GRIB/CSV/JSON; chuẩn hóa đơn vị, thời gian, tọa độ; lưu Parquet; tạo dữ liệu bổ sung thời tiết/baseline; viết Spark batch và benchmark.  
**Người 3 — Event & Visualization:** chạy event detection từ bảng curated theo hợp đồng, tạo JSON sự kiện; dashboard **chỉ đọc event contract**, không phụ thuộc việc API của người 1 đang online.

Mỗi người viết unit tests/documentation cho phần của mình. **Các file dùng chung** `contracts/`, `configs/`, `pyproject.toml` hoặc `requirements*` chỉ thay đổi bằng Pull Request được review.

## 4. Hợp đồng lưu trữ + chạy module

### 4.1. Cấu trúc đường dẫn (có thể cấu hình root, không hard-code `C:\Users\Admin`)

```text
configs/project.yaml                # tham số công khai
contracts/                         # schema + fixture spec
src/ingestion/                     # người 1
src/processing/                    # người 2
src/events/                        # người 3
src/dashboard/                     # người 3
src/common/                        # sửa chung qua review
data/raw/{source}/...               # nguồn gốc + manifest, không sửa payload
data/silver/{table}/...             # parquet chuẩn hóa
data/curated/{table}/...            # baseline/enriched parquet
data/events/events.json            # event API cho dashboard
tests/                              # mỗi module có tests
fixtures/                           # sample nhỏ, đủ kiểm thử; chỉ commit khi giấy phép cho phép
experiments/                        # các script kiểm chứng nguồn
reports/                            # ghi benchmark, quality
```

Các đường dẫn dưới `data/` là **đầu ra local**, không push dataset lớn lên GitHub. Mỗi lần ingestion ghi manifest riêng, ví dụ:

```json
{
  "source": "nasa_firms",
  "region_id": "VN_BBOX",
  "start_utc": "2024-11-01T00:00:00Z",
  "end_utc": "2024-11-02T00:00:00Z",
  "retrieved_at_utc": "<actual timestamp>",
  "format": "csv",
  "file": "<relative path under data/raw>",
  "status": "success",
  "row_count": null,
  "sha256": "<actual hash>"
}
```

Các giá trị trong ngoặc nhọn là **placeholder**, không phải dữ liệu thật. Manifest không chứa token hoặc URL có key. `status` chỉ nhận `success | empty | partial | failed`. Nếu query lỗi, phải ghi trạng thái/diagnostic, không giả ra dòng dữ liệu.

### 4.2. CLI dự kiến — interface phải giữ ổn định sau khi hiện thực

```powershell
python -m src.ingestion.cli --source era5 --region VN_BBOX --start 2024-11-01 --end 2024-11-02
python -m src.processing.cli --config configs/project.yaml
python -m src.events.cli --config configs/project.yaml
```

Các lệnh trên là **hợp đồng phải triển khai**, chưa phải chương trình đã tồn tại. Một lệnh thành công phải trả exit code 0, tạo output đúng đường dẫn; lỗi phải trả mã khác 0 và thông báo rõ. Ba module không được phụ thuộc vào đường dẫn tuyệt đối hoặc API key của một thành viên cụ thể.

## 5. Data Contract v1 — bảng chuẩn hóa (Silver)

**Quy tắc chung:** tên cột `snake_case`; timestamp lưu UTC (ISO-8601 có `Z` khi JSON; Spark session timezone `UTC`); lat/lon số thực EPSG:4326; `null` khác `0`; giá trị thiếu/cờ QC được bảo toàn; mỗi bảng có `source` và provenance tới nguồn gốc. Nếu trường chưa có ở source, ghi `null`, không tự bịa. Bảng Parquet có kiểu dữ liệu cố định và kiểm tra uniqueness theo khóa quy định.

### 5.1. `air_quality_hourly` — người 2 tạo từ raw OpenAQ

| Cột | Kiểu | Bắt buộc | Nghĩa |
|---|---|---|---|
| `sensor_id` | string | Có | ID sensor OpenAQ, ví dụ `21632` |
| `location_id` | string | Có | ID location OpenAQ, ví dụ `7441` |
| `timestamp_utc` | timestamp | Có | **Đầu** khoảng đo 1 giờ |
| `period_end_utc` | timestamp | Có | Cuối khoảng đo (exclusive) |
| `latitude` / `longitude` | double | Có | Lấy từ metadata location nếu bản ghi đo không có coordinates |
| `parameter` | string | Có | v1 nhận `pm25` hoặc `pm10` |
| `value_ugm3` | double, nullable | Có | Nồng độ; được phép `null` |
| `has_flags` | boolean, nullable | Có | OpenAQ `flagInfo.hasFlags`; `true` **không tự đồng nghĩa invalid** |
| `source` | string | Có | Giá trị `openaq` |

**Unique key:** `(sensor_id, parameter, timestamp_utc)`. Nếu có nhiều bản ghi cùng key, deduplicate theo quy tắc có log, không âm thầm cộng dồn. Những dữ liệu chất khác PM2.5/PM10 chưa thuộc v1. **Không tự bỏ `null` hoặc flagged ở silver.**

### 5.2. `weather_hourly` — người 2 tạo từ ERA5

| Cột | Kiểu | Bắt buộc | Nghĩa |
|---|---|---|---|
| `grid_id` | string | Có | ID ổn định cho điểm lưới ERA5 theo tọa độ chuẩn |
| `timestamp_utc` | timestamp | Có | Mốc giờ UTC |
| `latitude` / `longitude` | double | Có | Tâm điểm lưới EPSG:4326 |
| `temperature_c` | double, nullable | Có | `2t` từ Kelvin → °C |
| `wind_u_ms` / `wind_v_ms` | double, nullable | Có | Các thành phần gió 10 m; null nếu chưa tải |
| `dewpoint_c` | double, nullable | Có | Nhiệt độ điểm sương; null nếu chưa tải |
| `precipitation_mm_1h` | double, nullable | Có | Chỉ điền khi đã xử lý/kiểm tra đúng ý nghĩa tích lũy ERA5; nếu chưa, null |
| `source` | string | Có | Giá trị `era5` |

**Unique key:** `(grid_id, timestamp_utc)`. **Không biến `null` thành 0**. Trong sample đầu v1 mới có `temperature_c` là bình thường. Điểm lưới trong bounding box có thể nằm ngoài Việt Nam; không gán mọi điểm là lãnh thổ Việt Nam.

### 5.3. `fire_hotspots` — người 2 tạo từ NASA FIRMS

| Cột | Kiểu | Bắt buộc | Nghĩa |
|---|---|---|---|
| `hotspot_id` | string | Có | ID deterministic từ bộ trường nguồn ổn định; quy tắc hash phải được ghi trong code |
| `timestamp_utc` | timestamp | Có | Ghép `acq_date` + `acq_time` (giữ đúng giờ UTC từ FIRMS) |
| `latitude` / `longitude` | double | Có | Tọa độ hotspot |
| `frp_mw` | double, nullable | Có | FRP, đơn vị MW |
| `confidence_raw` | string, nullable | Có | Bảo toàn category/gốc của dataset, không giả định là % |
| `satellite` | string | Có | Tên vệ tinh |
| `instrument` | string, nullable | Có | Instrument |
| `daynight` | string, nullable | Có | Giá trị D/N nếu có |
| `source_product` | string | Có | Ví dụ archive hay NRT + sản phẩm tương ứng |
| `source` | string | Có | Giá trị `nasa_firms` |

**Unique key:** `hotspot_id`; không tự coi mỗi hotspot là một vụ cháy, cũng không tự loại các quan sát gần nhau là trùng. Với dataset confidence khác nhau cần diễn giải đúng nguồn trước khi lọc.

## 6. Data Contract v1 — bảng bàn giao Processing → Events (Curated)

Người 2 xuất ba dataset này (Parquet) để người 3 chạy độc lập, có thể bắt đầu bằng fixture tổng hợp:

1. **`air_quality_context_hourly`** = toàn bộ cột `air_quality_hourly` + các cột nullable: `weather_grid_id`, `weather_timestamp_utc`, `weather_distance_km`, `temperature_c`, `wind_u_ms`, `wind_v_ms`, `precipitation_mm_1h`. Dữ liệu AQ được **left join** với ERA5, không mất giờ vì không khớp weather.
2. **`fire_hotspots_context`** = toàn bộ cột `fire_hotspots` + các cột weather nullable tương tự. Không được làm rơi hotspot khi thiếu thời tiết.
3. **`air_quality_baseline`** = `sensor_id` (string), `parameter` (string), `timestamp_utc` (timestamp), `baseline_ugm3` (double nullable), `sample_count` (int), `method` (string), `is_usable` (boolean). Baseline phải tính bằng **dữ liệu quá khứ**, không sử dụng quan sát tương lai hoặc bản thân giờ đang xét. Chi tiết window/min sample chốt ở config sau khi đo data quality.

**Quy tắc spatial–temporal join:** dùng điểm ERA5 gần nhất và cùng mốc giờ UTC hoặc phương pháp alignment được nhóm ghi rõ; cấu hình tối đa khoảng cách/lệch giờ, lưu `weather_distance_km` và `weather_timestamp_utc`. **Không kết luận nguyên nhân** từ sự trùng thời gian/không gian. Missing weather phải để null và ghi trạng thái, không bịa context.

## 7. Event Contract v1 — Events → Dashboard

**Artifact bắt buộc:** `data/events/events.json` là **JSON array** các event đúng schema dưới đây. Người 3 có thể lưu thêm Parquet, nhưng dashboard chỉ được phụ thuộc vào JSON contract v1.

| Cột JSON | Kiểu | Quy tắc |
|---|---|---|
| `event_id` | string | ID deterministic theo rule và version |
| `event_type` | enum | `AIR_QUALITY_ANOMALY` / `FIRE_HOTSPOT_CLUSTER` |
| `region_id` | string | Mã vùng từ configs |
| `start_time_utc` / `end_time_utc` | ISO-8601 string | UTC, end exclusive |
| `centroid_latitude` / `centroid_longitude` | number | Vị trí đại diện sự kiện |
| `severity` | enum | `low`, `medium`, `high`, `undetermined` (không tự gán nếu chưa có rule) |
| `metrics` | object | Chỉ số định lượng theo event type |
| `weather_context` | object/null | Ngữ cảnh khí tượng có bằng chứng; null nếu không đủ dữ liệu |
| `evidence_ids` | string[] | ID measurement/hotspot hoặc tham chiếu chứng cứ |
| `algorithm_version` | string | VD `aq-v1`, `fire-v1` |

**`metrics` theo loại event (tối thiểu):**

- `AIR_QUALITY_ANOMALY`: `sensor_ids` (string[]), `parameter` (`pm25` v1), `peak_ugm3` (number), `mean_ugm3` (number), `baseline_ugm3` (number), `duration_hours` (number). Không tạo event nếu chưa có baseline đủ điều kiện theo thuật toán đã công bố.
- `FIRE_HOTSPOT_CLUSTER`: `hotspot_count` (int), `max_frp_mw` (number/null), `sum_frp_mw` (number/null), `satellites` (string[]). Không xem một điểm đơn lẻ là cluster.

**JSON minh họa — SYNTHETIC, KHÔNG PHẢI SỰ KIỆN THẬT:**

```json
[
  {
    "event_id": "demo-aq-001",
    "event_type": "AIR_QUALITY_ANOMALY",
    "region_id": "HANOI",
    "start_time_utc": "2024-11-01T10:00:00Z",
    "end_time_utc": "2024-11-01T14:00:00Z",
    "centroid_latitude": 21.021939,
    "centroid_longitude": 105.818806,
    "severity": "undetermined",
    "metrics": {
      "sensor_ids": ["21632"],
      "parameter": "pm25",
      "peak_ugm3": 90.0,
      "mean_ugm3": 75.0,
      "baseline_ugm3": 30.0,
      "duration_hours": 4
    },
    "weather_context": null,
    "evidence_ids": ["mock-001", "mock-002"],
    "algorithm_version": "aq-v1"
  }
]
```

Dashboard cần xử lý được array rỗng `[]`, event có `weather_context=null`, và cột `severity=undetermined`. LLM (nếu có) chỉ nhận structured event, không tự tính anomaly hay suy diễn nhân quả.

## 8. Cấu hình chung và môi trường

**`configs/project.yaml`** (ví dụ, không chứa secrets):

```yaml
project:
  timezone: UTC
  crs: EPSG:4326
  data_root: data
  environment: local
regions:
  VN_BBOX:
    north: 24.0
    west: 102.0
    south: 8.0
    east: 110.0
  HANOI:
    north: 21.25
    west: 105.5
    south: 20.75
    east: 106.0
processing:
  spark_master: 'local[2]'
  spark_driver_memory: 4g
  timezone: UTC
integration:
  start_utc: null    # sẽ chốt sau khi xác thực khoảng trùng nguồn
  end_utc: null
```

Bất cứ khi nào thử thực tế, CLI truyền window và region rõ ràng; không hard-code năm/vùng cố định trong source code. Python 3.12.x được chốt; người 2 xác minh Java tương thích với Spark version đã chọn trước khi cả nhóm pin dependency. Dependencies đặt tại một nơi chung, version cần pin sau khi chạy thành công trên máy nhóm; không tự cài các phiên bản xung đột vào global Python.

## 9. Fixtures và cách làm song song

- `fixtures/raw/`: trích sample thật **nhỏ và được phép chia sẻ** từ OpenAQ/FIRMS/ERA5; gắn nhãn `real_sample`. Nếu giấy phép/attribution chưa rõ, chỉ để local hoặc ghi cách lấy, **không push** lên repo.
- `fixtures/silver/`: CSV/Parquet **synthetic** đúng schema, gồm cả null, dữ liệu bị gắn cờ, tọa độ/giờ, trường hợp rỗng.
- `fixtures/events/events_mock.json`: JSON synthetic theo event contract để người 3 làm dashboard ngay.
- Người 1 test ingest độc lập; người 2 test chuẩn hóa bằng raw samples/fixtures; người 3 test events và dashboard bằng curated/event fixtures.
- Khi nối dữ liệu thật, thay **input**; không tự ý đổi schema đầu ra. Fixture luôn dán nhãn **SYNTHETIC — TEST ONLY**, không dùng làm kết quả benchmark/học thuật.

## 10. Git, kiểm thử và điều kiện merge

```text
main              # ổn định
  └── develop     # tích hợp đã kiểm thử
       ├── feat/ingestion
       ├── feat/processing
       └── feat/events-dashboard
```

**Nguyên tắc:** Mỗi nhánh tạo từ `develop`; mở PR nhỏ lên `develop` thường xuyên, **không đợi đến cuối kỳ mới merge**. Ít nhất 1 người khác review các PR sửa contracts/configs. Sau khi end-to-end pass mới merge `develop` → `main`. Nếu hợp đồng thay đổi breaking: mở issue, thông báo cả nhóm, nâng `v1.x → v2` hoặc thống nhất migration và cập nhật fixture/test.

**Definition of Done — mỗi PR:**

1. Có hướng dẫn chạy và input/output minh họa; **không chứa key/token** hoặc dữ liệu lớn.
2. Chạy được trên `.venv` Python 3.12; dependency cập nhật có kiểm soát.
3. Test pass, ít nhất có case bình thường, empty/null và dữ liệu lỗi.
4. Xuất đúng schema v1 (tên cột, kiểu, UTC, đơn vị); kiểm tra unique key.
5. Không sửa file/contract chung thiếu review; không làm hỏng module đã merge vào `develop`.

**MVP end-to-end được coi là chạy được khi:**

- Có lệnh chạy theo thứ tự **ingestion → processing → events → dashboard** trên dữ liệu **nhỏ, thật**, và lưu được đầy đủ kết quả trung gian.
- Hai event type có bài test logic trên fixture và có đầu ra đúng JSON contract; đánh giá trên dữ liệu thật phải được báo cáo riêng.
- Có kiểm thử join không gian–thời gian trên một window thực tế phù hợp; nếu một nguồn không có quan sát trong cửa sổ đó, phải ghi rõ giới hạn thay vì giả kết quả.
- Spark local chạy được trên máy RAM 16 GB; có benchmark được đo với dataset thật/đại diện. Không tuyên bố Spark nhanh hơn trước khi đo.

## 11. Phần còn mở, cần nhóm xác nhận trong cuộc họp đầu tiên

1. **Ai nhận Người 1/2/3**, ai phụ trách review và phối hợp merge?
2. **Integration window** nào có đồng thời dữ liệu nguồn phù hợp? Gợi ý bắt đầu khảo sát một khoảng trong **2024**, vì OpenAQ đã có measurement thật; FIRMS archive và ERA5 cùng khoảng **phải xác minh**, chưa khẳng định.
3. **Spark/PySpark + Java phiên bản nào** chạy ổn trên 3 máy? Người 2 kiểm tra, báo nhóm rồi mới pin.
4. Ngưỡng QC `has_flags`, điều kiện baseline usable, khoảng cách join, tham số cluster phải được mô tả và versioned **khi có đủ dữ liệu**.
5. Ai duy trì README, `contracts/`, `configs/` và chịu trách nhiệm chấp thuận thay đổi breaking?

---

**Quy ước ký duyệt:** Mỗi thành viên phản hồi `ĐỒNG Ý CONTRACT v1.0` sau khi đọc. Nếu cần sửa, ghi chính xác mục/cột/ý nghĩa muốn đổi **trước khi tách nhánh**. Sau khi đồng ý, mọi sửa đổi tương thích thì qua PR; sửa đổi phá vỡ hợp đồng phải được cả nhóm chấp thuận.
