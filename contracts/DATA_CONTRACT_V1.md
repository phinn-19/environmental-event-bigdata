# DATA CONTRACT v1.0 — Environmental Event Big Data

**Trạng thái:** **ĐỀ XUẤT ĐỂ CẢ NHÓM PHÊ DUYỆT**. Kế thừa `PROJECT_CONTRACT_V1.md`, dùng song song với `DATA_ACQUISITION_V1.md`.  
**Mục tiêu:** ba thành viên code độc lập nhưng input/output vẫn ăn khớp, không cần đợi pipeline trước hoàn chỉnh.  
**Phiên bản:** `data-contract-v1`; breaking change cần sự đồng ý của 3 thành viên.

## 0. Ai tạo gì, ai được dựa vào gì?

| Người | Module | Nhận | Bàn giao (hợp đồng này) |
|---|---|---|---|
| **1 — Ingestion** | `src/ingestion/` | CDS / NASA FIRMS / OpenAQ API | `data/raw/...` + `manifest.json` theo Acquisition v1 |
| **2 — Processing** | `src/processing/` | raw + manifest hoặc fixture raw | `data/silver/{table}/...parquet`, `data/curated/{table}/...parquet` |
| **3 — Events** | `src/events/` | các bảng curated (hoặc synthetic fixture đúng schema) | `data/events/events.json` |
| **3 — Dashboard** | `src/dashboard/` | **chỉ** `data/events/events.json` v1 | Giao diện đọc được cả JSON array rỗng |

**Cấm coupling:** Người 2 không cần gọi API key của Người 1; Người 3 không đọc raw GRIB; Dashboard không import nội bộ `src/processing`. Mọi đường dẫn là relative theo `data_root`, không chứa `C:\Users\Admin`.

## 1. Quy tắc dữ liệu xuyên suốt

| Quy chuẩn | Quy tắc bắt buộc |
|---|---|
| Đặt tên | bảng/cột `snake_case`, ví dụ `pm25_ugm3` |
| Timestamp trong Parquet | Spark `timestamp` (session timezone **UTC**); khi serialize JSON dùng ISO 8601 với hậu tố `Z` |
| Time window nội bộ | `[start_utc, end_utc)`; end exclusive; mỗi bản ghi có ý nghĩa thời gian rõ ràng |
| Tọa độ | WGS84/EPSG:4326; `latitude` ∈ [-90,90], `longitude` ∈ [-180,180] |
| Đơn vị | nhiệt độ °C, gió m/s, mưa mm/giờ, PM2.5/PM10 µg/m³, FRP MW |
| Dữ liệu thiếu | giữ `null` thật; không đổi thành 0; với float có thể có NaN, khi xuất JSON normalize thành null |
| Quality | giữ `has_flags`/`confidence_raw`; không xóa flagged trước khi đến tầng QC/feature calculation |
| Nguồn | mọi bảng có `source`; giữ manifest/raw path làm provenance qua metadata hoặc cột `raw_file` nếu triển khai |
| Duplicate | báo số lượng, deduplicate theo khóa ổn định; **không** mất dữ liệu ngầm |
| Độ phân giải | một dòng weather = một grid point/một validity hour; air-quality = một sensor/một khoảng giờ; FIRMS = một hotspot observation |
| Khi không có dữ liệu | xuất empty Parquet **đúng schema**; event output là `[]` |

**Kiểu dùng trong bảng:** `string`, `double`, `boolean`, `int`, `timestamp` là kiểu logic, ánh xạ tương ứng sang Spark/Parquet. Cột gắn `nullable` phải được giữ nullable, kể cả khi fixture không có null.

## 2. SILVER — chuẩn hóa từng nguồn

### 2.1. `data/silver/air_quality_hourly/` — OpenAQ

**Một dòng** = một sensor, một chất ô nhiễm, một khoảng đo một giờ. Metadata trạm lấy từ OpenAQ `locations`; measurement giờ từ `sensors/{id}/hours`.

| Cột | Kiểu | Null? | Bắt buộc / ý nghĩa |
|---|---|---|---|
| `sensor_id` | string | Không | ID OpenAQ, ví dụ `21632` |
| `location_id` | string | Không | ID location, ví dụ `7441` |
| `timestamp_utc` | timestamp | Không | **Đầu** khoảng đo `period.datetimeFrom.utc` |
| `period_end_utc` | timestamp | Không | **Cuối** khoảng `period.datetimeTo.utc`; thường `timestamp+1h` |
| `latitude` | double | Không | tọa độ theo location metadata, nếu thiếu thì đưa bản ghi vào quarantine thay vì bịa |
| `longitude` | double | Không | như trên |
| `parameter` | string | Không | `pm25` (MVP), `pm10` (mở rộng) |
| `value_ugm3` | double | **Có** | giá trị measured/aggregated hourly; **giữ null** |
| `has_flags` | boolean | **Có** | từ `flagInfo.hasFlags`; null khi field không có |
| `source` | string | Không | luôn `openaq` |

**Unique key:** `(sensor_id, parameter, timestamp_utc)`. Không coi `has_flags=true` là invalid mặc định. Cần kiểm tra đơn vị raw `parameter.units=µg/m³` trước khi ghi vào `value_ugm3`; khác đơn vị thì chuyển đổi có quy tắc hoặc cách ly, tuyệt đối không đổi nhãn đơn vị suông.

**Contract tests:** `period_end_utc > timestamp_utc`; cùng sensor/time không trùng; các giá trị `null` vẫn là một dòng; ghi nhận cả số giờ có timestamp và số giờ có giá trị không-null. `coverage.percentComplete` của OpenAQ **không** thay thế kiểm đếm valid values.

### 2.2. `data/silver/weather_hourly/` — ERA5

**Một dòng** = một điểm lưới ERA5 tại một **validity hour UTC**; dạng **wide**, 5 biến ở 5 cột, **không tự nhân 5 số dòng**.

| Cột | Kiểu | Null? | Ý nghĩa |
|---|---|---|---|
| `grid_id` | string | Không | ID điểm lưới ổn định theo lat/lon, resolution |
| `timestamp_utc` | timestamp | Không | validity time từ metadata GRIB |
| `latitude`, `longitude` | double | Không | tâm điểm lưới EPSG:4326 |
| `temperature_c` | double | Có | 2m temperature K−273.15 |
| `dewpoint_c` | double | Có | 2m dewpoint K−273.15 |
| `wind_u_ms`, `wind_v_ms` | double | Có | thành phần gió tại 10 m |
| `precipitation_mm_1h` | double | Có | lượng mưa 1 giờ **kết thúc** ở `timestamp_utc`, từ `tp_m ×1000` |
| `source` | string | Không | luôn `era5` |

**Unique key:** `(grid_id, timestamp_utc)`. Grid theo ERA5 single-level atmospheric reanalysis thường là 0,25°; `grid_id` có thể sinh deterministic từ **giá trị tọa độ chuẩn + grid_resolution**, không dựa vào thứ tự đọc GRIB. Độ phân giải/resampling được khai báo trong dataset metadata.

**Đặc biệt quan trọng về mưa:** `temperature_c/wind_*` là dữ liệu tại `timestamp_utc`, nhưng `precipitation_mm_1h` là tích lũy **(timestamp_utc - 1h, timestamp_utc]** theo ngữ nghĩa ERA5 hourly. KHÔNG suy diễn mưa của khoảng `[t,t+1h)` từ dòng `timestamp=t` nếu chưa dịch thời gian; mưa của khoảng ấy thuộc validity time `t+1h`. Nếu kiểm thử `tp`/metadata không đạt, để `precipitation_mm_1h=null` và đánh dấu lý do, không điền 0.

**Validation:** mỗi vị trí/thời điểm chỉ một dòng; không ghép nhầm các biến từ ngày/giờ khác; kiểm tra nhiệt độ K trước/sau chuyển; không dùng `min/max` trên bbox làm nhiệt độ trung bình toàn Việt Nam. Với pilot 15/01/2025 24 giờ, 1 biến, 2.145 điểm lưới, mẫu thực tế có 51.480 quan sát.

### 2.3. `data/silver/fire_hotspots/` — NASA FIRMS

**Một dòng** = một quan sát hotspot FIRMS, **chưa phải event**.

| Cột | Kiểu | Null? | Ý nghĩa |
|---|---|---|---|
| `hotspot_id` | string | Không | ID deterministic cho quan sát; cùng input → cùng ID |
| `timestamp_utc` | timestamp | Không | `acq_date` + `acq_time` theo UTC (`HHMM.zfill(4)`) |
| `latitude`, `longitude` | double | Không | tọa độ điểm nóng |
| `frp_mw` | double | Có | FRP (MW), không mặc định 0 nếu missing |
| `confidence_raw` | string | Có | ví dụ `l/n/h`; bảo toàn nguyên gốc |
| `satellite` | string | Không | tên/viết tắt vệ tinh theo raw |
| `instrument` | string | Có | instrument như `VIIRS` |
| `daynight` | string | Có | `D` hoặc `N` |
| `source_product` | string | Không | `VIIRS_NOAA20_SP` hoặc `VIIRS_NOAA20_NRT` |
| `source` | string | Không | luôn `nasa_firms` |

**Unique key:** `hotspot_id`. Quy tắc tạo ID v1 để mọi nhánh đồng nhất: `sha256` của chuỗi chuẩn `source_product|satellite|timestamp_utc_ISO_Z|latitude_to_5dp|longitude_to_5dp|version|bright_ti4|scan|track|frp` (giá trị các trường cuối lấy raw đã trim, thiếu dùng chuỗi rỗng). Quy tắc này là lựa chọn project, không phải ID NASA phát sẵn; ghi `id_algorithm_version=hotspot-key-v1` trong metadata. Hai dòng có cùng ID được coi là trùng **record theo rule v1** và phải đếm/log. Không gộp các hotspot gần nhau ở silver.

**Validation:** raw CSV đủ header; tọa độ hợp lệ; `acq_time=616` → `06:16 UTC` chứ không phải `61:6`; giữ lại `confidence_raw` và tên source product phục vụ QC.

## 3. CURATED — hợp đồng Processing → Event Detection

**File format:** Parquet, UTF-8 metadata nếu có; mỗi dataset là một folder đọc được bằng Spark. Người 2 viết contract tests và có thể xuất fixture synthetic nhỏ để người 3 chạy trước.

### 3.1. `data/curated/air_quality_context_hourly/`

Tất cả cột của `air_quality_hourly`, cộng các cột **nullable**:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `weather_grid_id` | string | điểm ERA5 ghép gần nhất |
| `weather_timestamp_utc` | timestamp | validity time ERA5 dùng cho T/gió |
| `weather_distance_km` | double | khoảng cách trạm AQ ↔ grid |
| `temperature_c`, `wind_u_ms`, `wind_v_ms` | double | ngữ cảnh khí tượng |
| `precipitation_mm_1h` | double | mưa cho **cùng khoảng AQ `[t,t+1h)`**, lấy ERA5 ở validity time `t+1h` |

**Join chính:** `LEFT JOIN` từ AQ sang ERA5; nhiệt độ/gió chọn grid gần nhất với validity time **`t=timestamp_utc`** (hoặc rule alignment ghi rõ trong config), mưa ghép theo `validity_time=t+1h`. `weather_timestamp_utc` ghi mốc ERA5 của **T/gió**; không đại diện cho mốc cuối khoảng mưa. Nếu không đủ ngữ cảnh, các cột weather để null nhưng vẫn giữ hàng AQ.

### 3.2. `data/curated/fire_hotspots_context/`

Toàn bộ `fire_hotspots`, cộng `weather_grid_id`, `weather_timestamp_utc`, `weather_distance_km`, `temperature_c`, `wind_u_ms`, `wind_v_ms`, `precipitation_mm_1h` (các cột added nullable).

**Join chính:** chọn grid gần nhất và validity time ERA5 gần nhất theo giờ với hotspot; max distance/time offset là tham số config, **chưa mặc định một ngưỡng**. Với mưa, chỉ ghép khi định nghĩa khoảng giờ phù hợp đã được kiểm thử. Không loại hotspot nếu không join được khí tượng.

### 3.3. `data/curated/air_quality_baseline/`

| Cột | Kiểu | Null? | Ý nghĩa |
|---|---|---|---|
| `sensor_id`, `parameter` | string | Không | sensor và pollutant |
| `timestamp_utc` | timestamp | Không | thời điểm cần so sánh |
| `baseline_ugm3` | double | Có | baseline từ **dữ liệu trước thời điểm t**, không nhìn tương lai |
| `sample_count` | int | Không | số measurement hợp lệ được sử dụng |
| `method` | string | Không | cách ước lượng, versioned |
| `is_usable` | boolean | Không | baseline đủ điều kiện theo ngưỡng config |

**Unique key:** `(sensor_id, parameter, timestamp_utc)`. Không đưa bản ghi `value=null` vào baseline. Với flagged data cần có QC policy **công bố**; trước khi chốt, khuyến nghị mặc định không tính flagged vào baseline, nhưng vẫn bảo tồn ở silver và báo số lượng bị loại.

**Acceptance của Người 2:** đọc được dữ liệu fixture và raw thật; tạo đủ 3 folder silver + 3 folder curated đúng schema (được phép rỗng); demo join và baseline trên sample; kiểm tra không làm mất row của AQ/FIRMS nếu thời tiết thiếu. Spark session timezone UTC.

## 4. EVENTS — hợp đồng Event Detection → Dashboard

**Artifact cố định:** `data/events/events.json` chứa **JSON array** (không phải NDJSON). Cả khi không có event phải có file `[]`. JSON tuyệt đối không chứa `NaN`, `Infinity`, timezone không xác định.

| Field | Kiểu JSON | Bắt buộc | Rule |
|---|---|---|---|
| `event_id` | string | Có | stable ID theo algorithm + input/time/region |
| `event_type` | enum | Có | `AIR_QUALITY_ANOMALY`, `FIRE_HOTSPOT_CLUSTER` |
| `region_id` | string | Có | từ config |
| `start_time_utc`, `end_time_utc` | ISO-8601 string | Có | `Z`, end exclusive, start < end |
| `centroid_latitude`, `centroid_longitude` | number | Có | centroid/đại diện EPSG:4326 |
| `severity` | enum | Có | `low`, `medium`, `high`, `undetermined` |
| `metrics` | object | Có | schema tùy `event_type` |
| `weather_context` | object hoặc null | Có | không có context → null |
| `evidence_ids` | string[] | Có | id chứng cứ có thể truy ngược |
| `algorithm_version` | string | Có | ví dụ `aq-v1`/`fire-v1` |

### 4.1. `AIR_QUALITY_ANOMALY`

`metrics` yêu cầu: `sensor_ids` (string[], không rỗng), `parameter` (`pm25` ở MVP), `peak_ugm3` (number), `mean_ugm3` (number), `baseline_ugm3` (number), `duration_hours` (number > 0). Chỉ phát event nếu có baseline `is_usable=true`, một chuỗi/nhóm observation đáp ứng rule thuật toán, không dùng **một** spike đơn lẻ làm event.

**AQ evidence ID đề xuất:** `aq:{sensor_id}:{parameter}:{timestamp_utc_ISO_Z}` để người 3 có thể đối chiếu với `air_quality_hourly` (source key). Không cần người 1 phát ID này trong raw.

### 4.2. `FIRE_HOTSPOT_CLUSTER`

`metrics` yêu cầu: `hotspot_count` (integer >= 2), `max_frp_mw` (number|null), `sum_frp_mw` (number|null), `satellites` (string[]). `evidence_ids` tham chiếu `hotspot_id` của các điểm trong cụm. Việc `frp=null` không có nghĩa là `0`; nếu không có bất kỳ FRP hợp lệ nào thì `sum_frp_mw/max_frp_mw=null`.

### 4.3. `weather_context` v1

Nếu có: object có các khóa `temperature_c`, `wind_u_ms`, `wind_v_ms`, `precipitation_mm_1h` (**mỗi khóa number|null**), và `context_method` (string, ví dụ `nearest_grid_hourly_v1`). Các trường này chỉ là mô tả quan sát và **không** khẳng định một hotspot gây ra PM2.5 cao.

**Dashboard phải chịu được:** `[]`, `weather_context=null`, `severity="undetermined"`, `metrics` khác nhau giữa hai event type, evidence rỗng (nếu thuật toán định nghĩa cần thì event phải bị reject ngay ở tầng event), các giá trị optional null. Giao diện không trực tiếp gọi CDS/FIRMS/OpenAQ.

### 4.4. Ví dụ fixture SYNTHETIC (không phải dữ liệu quan trắc/event có thật)

```json
[
  {
    "event_id": "synthetic-aq-001",
    "event_type": "AIR_QUALITY_ANOMALY",
    "region_id": "HANOI",
    "start_time_utc": "2024-11-01T10:00:00Z",
    "end_time_utc": "2024-11-01T14:00:00Z",
    "centroid_latitude": 21.021939,
    "centroid_longitude": 105.818806,
    "severity": "undetermined",
    "metrics": {
      "sensor_ids": ["21632"], "parameter": "pm25",
      "peak_ugm3": 90.0, "mean_ugm3": 75.0,
      "baseline_ugm3": 30.0, "duration_hours": 4
    },
    "weather_context": null,
    "evidence_ids": ["aq:21632:pm25:2024-11-01T10:00:00Z"],
    "algorithm_version": "aq-v1"
  }
]
```

## 5. Folder/CLI bàn giao (interface dự kiến phải implement)

```text
configs/project.yaml                 # shared public config
contracts/DATA_CONTRACT_V1.md         # doc này
contracts/DATA_ACQUISITION_V1.md      # thông số raw ingestion
fixtures/events/events_mock.json      # synthetic, commit được
src/ingestion/                       # người 1
src/processing/                      # người 2
src/events/                          # người 3
src/dashboard/                       # người 3
data/raw/{source}/...                 # không push lên Git thường
<data_root>/silver/{table}/*.parquet  # đầu ra người 2
data/curated/{table}/*.parquet        # đầu ra người 2
data/events/events.json              # đầu ra người 3
```

```powershell
python -m src.ingestion.cli --source openaq --region VN_BBOX --start 2024-11-01 --end 2024-11-02
python -m src.processing.cli --config configs/project.yaml
python -m src.events.cli --config configs/project.yaml
```

**Đây là CLI contract**, các lệnh không có nghĩa là code hiện đã tồn tại. Thành công exit 0, lỗi exit !=0 và log sanitize. File share thay đổi path/schema phải sửa tài liệu và test fixture đồng thời.

## 6. Contract tests tối thiểu trước mỗi Pull Request

| Module | Cần kiểm tra |
|---|---|
| Ingestion | manifest đủ field; success/empty/partial/failed; secrets không lộ; thực sự có raw file |
| Processing AQ | parse đúng `period.datetimeFrom.utc`, null + flag, unique key, đơn vị µg/m³ |
| Processing weather | 5 biến mapping đúng; K→°C; hourly precipitation đúng validity-end; không nhân bản dòng khi ghép các biến |
| Processing FIRMS | `acq_time` zero-pad; confidence_raw text; ID deterministic; spatial coordinate valid |
| Curated integration | left join không làm giảm row AQ/hotspots; missing context phải null; không join nhầm mưa một giờ |
| Events | tạo JSON array hợp schema, hai `event_type` có fixtures; empty output `[]` |
| Dashboard | đọc JSON giả khi processing/ingestion chưa chạy; không phụ thuộc API keys |
| E2E | ingest sample nhỏ → silver/curated → events → dashboard; đo được coverage/row count/output path |

**Fixtures:** `fixtures/raw/` có thể dùng real samples chỉ khi giấy phép/attribution cho phép, hoặc tải local bằng script; `fixtures/silver/`, `fixtures/curated/`, `fixtures/events/` dùng **SYNTHETIC TEST ONLY** và luôn gắn nhãn. Cấm dùng synthetic fixtures để chứng minh dữ liệu thật/hiệu năng Spark.

## 7. Quy tắc thay đổi hợp đồng

1. Cả nhóm duyệt v1 trước khi rẽ 3 nhánh từ `develop`.
2. Mỗi thành viên chịu trách nhiệm thư mục `src/` của mình; file `configs/`, `contracts/`, dependency được sửa bằng PR và ít nhất một người khác review.
3. Muốn thêm cột optional backward-compatible: PR + sửa schema/tests/docs. Muốn đổi tên cột, ý nghĩa, timestamp alignment, kiểu dữ liệu, event JSON: **breaking change**, cần ba thành viên đồng ý và migration/fixture update.
4. Merge PR nhỏ từng phần vào `develop`; chạy integration test thường xuyên; chỉ đưa `develop` lên `main` sau khi E2E ổn định.

## 8. Những quyết định CỐ Ý để mở (không chặn việc code song song)

- Window lịch sử đồng bộ thật: xác minh dữ liệu NASA FIRMS NOAA-20 SP/NRT + OpenAQ hourly + ERA5 trong **cùng ngày** trước khi demo tích hợp 3 nguồn.
- Danh sách sensor thực tế, mức đầy đủ dữ liệu PM2.5, license/attribution theo provider OpenAQ.
- Rule QC `has_flags` chi tiết; baseline window và ngưỡng min samples; phép phát hiện anomaly; thuật toán/epsilon/time-window clustering; ngưỡng phân loại severity.
- Ngưỡng khoảng cách/giờ spatial-temporal join; benchmark Spark/DuckDB; số năm historical; Java và PySpark pin version; dữ liệu đủ lớn để chứng minh scaling.
- LLM/Kafka/vector database/database server không nằm trong MVP core.

**Tất cả mục mở phải được giao người sở hữu và viết issue để xử lý trong triển khai; không dùng giá trị tùy tiện rồi gọi là đã được xác nhận.**

## 9. Tài liệu nền (truy nguồn)

- `PROJECT_CONTRACT_V1.md` (nội bộ nhóm): scope và Git workflow.
- `DATA_ACQUISITION_V1.md`: source fields, raw metadata và API window.
- Copernicus ERA5 hourly & ECMWF accumulated variables: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels ; https://confluence.ecmwf.int/pages/viewpage.action?pageId=185081924
- NASA FIRMS area/archive: https://firms.modaps.eosdis.nasa.gov/api/area/ ; https://firms.modaps.eosdis.nasa.gov/download/
- OpenAQ docs (measurements/location): https://docs.openaq.org/resources/measurements ; https://docs.openaq.org/resources/locations

---

**Câu chốt họp nhóm:** *Cả 3 người có đồng ý rằng Người 1 bàn giao RAW + manifest, Người 2 bàn giao các bảng Silver/Curated đúng tên và schema v1, Người 3 bàn giao Event JSON v1; và các thay đổi contract phải có PR/review không?* Nếu có, nhóm có thể bắt đầu code song song ngay bằng fixtures.
