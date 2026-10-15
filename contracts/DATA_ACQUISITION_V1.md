# DATA ACQUISITION SPECIFICATION v1.0

**Project:** Xây dựng hệ thống thu thập, tích hợp và phân tích sự kiện môi trường từ dữ liệu quan trắc, khí tượng và viễn thám tại Việt Nam  
**Trạng thái:** **ĐỀ XUẤT ĐỂ BA THÀNH VIÊN PHÊ DUYỆT**, chưa phải thông số đã kiểm chứng toàn bộ.  
**Áp dụng cùng:** `PROJECT_CONTRACT_V1.md` và `DATA_CONTRACT_V1.md`.  
**Vai trò chính:** Người 1 (Ingestion); Người 2 (Processing) và Người 3 (Events) có quyền review.  
**Ngày đối chiếu tài liệu công khai:** 13/10/2026.

## A. Phân biệt việc đã xác nhận và quyết định thiết kế

- **ĐÃ TEST BẰNG DỮ LIỆU THẬT:** OpenAQ API v3 trả metadata của trạm Hà Nội `location_id=7441`, PM2.5 `sensor_id=21632` (24 giờ, 22 giá trị không null); FIRMS VIIRS NOAA-20 NRT trả 10 điểm nóng trong một truy vấn gần đây; CDS ERA5 `2m_temperature` một ngày trong bbox Việt Nam trả 24 × 2.145 = 51.480 điểm lưới, GRIB 105.552 bytes.
- **CHỈ LÀ QUYẾT ĐỊNH MVP:** chọn biến khí tượng nào, schema, tên file, danh mục sản phẩm, quy tắc phân trang, các window dùng để triển khai.
- **CHƯA XÁC NHẬN:** OpenAQ có bao nhiêu trạm đủ dữ liệu liên tục; FIRMS NOAA-20 SP/NRT có sẵn **đúng ngày lịch sử** hay không; ba nguồn có window giao nhau đủ tốt hay không; quy mô sau chuyển sang Parquet. Không được đổi nhãn các mục này thành PASS nếu chưa test.

## 1. Phạm vi truy xuất thống nhất

| Thuộc tính | Chốt cho v1 |
|---|---|
| Domain | Việt Nam; hệ thống lưu `region_id`, không hard-code địa lý trong script |
| `VN_BBOX` | North=24.0, West=102.0, South=8.0, East=110.0 |
| `HANOI` (pilot) | North=21.25, West=105.5, South=20.75, East=106.0 |
| Khoảng thời gian | UTC, dạng `[start_utc, end_utc)` (end exclusive) trên interface nội bộ |
| Integration pilot ứng viên | `[2024-11-01T00:00:00Z, 2024-11-08T00:00:00Z)` **CHƯA CHỐT**; chỉ dùng nếu xác minh đủ dữ liệu ở 3 nguồn |
| Chiến lược | Batch-first: tải từng ngày/tháng, có log manifest và hỗ trợ rerun |
| Quy tắc không gian | BBOX là vùng chữ nhật, **không** coi mọi điểm trong BBOX là lãnh thổ Việt Nam |
| Chính sách máy 16 GB | Dùng 1 ngày/vài ngày để develop; theo dõi bytes; mở rộng khi pipeline chạy ổn |
| Mốc chạy | Tránh request quá lớn; ghi trạng thái `success`, `empty`, `partial`, `failed` |

**Quy tắc chuyển tham số theo API:** nội bộ yêu cầu end-exclusive; API FIRMS nhận **ngày bắt đầu + số ngày (1–5)**, có tính ngày cuối vào kết quả. Ingestion phải chia đúng các ngày UTC nằm trong `[start,end)`, tuyệt đối không lấy dư ngày rồi ghi sai window.

## 2. ERA5 / Copernicus CDS

**Nguồn sản phẩm v1:** `reanalysis-era5-single-levels`; `product_type=reanalysis`; hourly; dùng CDS API (`cdsapi`); giữ **GRIB** làm raw. Dùng khu vực `[N, W, S, E]` trong CDS, không nhầm với FIRMS `[W, S, E, N]`.

### 2.1. Biến cần tải

| Mức | CDS API `variable` | Ký hiệu GRIB thường gặp | Đơn vị gốc | Silver nhận | Mục đích |
|---|---|---|---|---|---|
| **MUST** | `2m_temperature` | `2t` | K | `temperature_c` | Nhiệt độ, ngữ cảnh khí tượng |
| **MUST** | `2m_dewpoint_temperature` | `2d` | K | `dewpoint_c` | Ngữ cảnh ẩm, phân tích bổ sung |
| **MUST** | `10m_u_component_of_wind` | `10u` | m/s | `wind_u_ms` | Hướng/thành phần gió |
| **MUST** | `10m_v_component_of_wind` | `10v` | m/s | `wind_v_ms` | Hướng/thành phần gió |
| **MUST** | `total_precipitation` | `tp` | m nước tương đương | `precipitation_mm_1h` | Ngữ cảnh lượng mưa theo giờ |
| OPTIONAL | `surface_pressure` | `sp` | Pa | Chưa có trong schema v1 | Chỉ mở rộng khi có lý do |

**Chia giai đoạn tải:** giai đoạn lập trình đầu tiên dùng một ngày + `2m_temperature` để kiểm tra decoder; trước khi nghiệm thu Ingestion v1 phải tải được **cả 5 biến MUST** (có thể thành nhiều GRIB file nhỏ) với cùng region/window. Một file không có biến nào thì phải báo `partial`/`failed`, không tự gán số 0.

**Quy tắc thời gian và đơn vị:**

- `2t`,`2d`: K → °C bằng `K - 273.15`; `10u`,`10v` giữ m/s.
- Với **ERA5 hourly reanalysis** từ CDS, `total_precipitation` là lượng tích lũy **trong một giờ kết thúc tại validity time UTC**; đổi `m × 1000 = mm`, **không tính hiệu hai giá trị giờ liên tiếp như ERA5-Land**. Mốc `00:00` phản ánh giờ kết thúc lúc 00:00 (thuộc ngày trước đó theo thời điểm bắt đầu). Pipeline phải ghi nhận rõ alignment (xem `DATA_CONTRACT_V1.md`). Trước khi điền trường mưa, người 2 xác minh metadata `stepType/stepRange` của GRIB thực tế.
- Thời gian file GRIB phải lấy từ metadata hợp lệ, không lấy từ tên file.

**Ví dụ request mẫu (không chứa token):**

```python
DATASET = "reanalysis-era5-single-levels"
REQUEST = {
    "product_type": ["reanalysis"],
    "variable": [
        "2m_temperature", "2m_dewpoint_temperature",
        "10m_u_component_of_wind", "10m_v_component_of_wind",
        "total_precipitation",
    ],
    "year": ["2024"], "month": ["11"], "day": ["01"],
    "time": [f"{hour:02d}:00" for hour in range(24)],
    "area": [24.0, 102.0, 8.0, 110.0],
    "data_format": "grib",
    "download_format": "unarchived",
}
```

**Chính sách xuất raw:** `data/raw/era5/<region_id>/year=YYYY/month=MM/day=DD/<request_id>.grib` + manifest. Mỗi request ghi số ngày, biến, GRIB message count nếu đọc được, byte size, checksum; không phụ thuộc tên file để phân biệt biến.

## 3. NASA FIRMS

**Nguồn v1:** VIIRS NOAA-20 (`VIIRS_NOAA20_SP` ưu tiên cho lịch sử đã được xử lý chuẩn; `VIIRS_NOAA20_NRT` cho dữ liệu mới/còn trong cửa sổ NRT). Không dùng `NRT` 2026 để giả vờ thay cho historical 2024. Nếu cần NOAA-21 về sau: mở rộng **OPTIONAL**, phải duyệt mapping schema.

### 3.1. Raw CSV: giữ toàn bộ 14 trường trả về

| Trường gốc | Bắt buộc có trong header | Mục đích / xử lý |
|---|---|---|
| `latitude`, `longitude` | Có | EPSG:4326, kiểm tra miền giá trị |
| `acq_date`, `acq_time` | Có | ghép ngày + HHMM **UTC**; `acq_time` cần `zfill(4)` |
| `satellite`, `instrument`, `version` | Có | nhận dạng sản phẩm/phiên bản |
| `confidence` | Có | **giữ giá trị gốc dạng text**; VIIRS có thể `l/n/h`, không mặc định % |
| `frp` | Có | MW; nullable nếu thiếu/không parse được |
| `daynight` | Có | `D`/`N`, giữ nguyên gốc |
| `bright_ti4`, `bright_ti5` | Có | đặc trưng brightness temperature; chưa dùng làm ngưỡng v1 |
| `scan`, `track` | Có | kích thước pixel, giúp QC/giải thích clustering |

**Không bắt buộc có hotspot trong mỗi ngày.** HTTP thành công + CSV chỉ có header = `empty`, khác `failed`. Giữ nguyên raw CSV, không xóa hotspot trùng tọa độ trước khi lưu silver.

### 3.2. API/Archive và giới hạn

- Trước khi tải lịch sử: gọi **`/api/data_availability`** để xem ngày khả dụng theo `VIIRS_NOAA20_SP` và `VIIRS_NOAA20_NRT`; nguồn có thể chuyển từ NRT sang SP theo thời gian.
- FIRMS Area API: `https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{W,S,E,N}/{DAY_RANGE}/{YYYY-MM-DD}`. `DAY_RANGE` từ 1 đến 5; `{DATE}` là **ngày bắt đầu** của đợt. Không ghi URL có MAP_KEY vào log/manifest/error message.
- Nếu dữ liệu lịch sử không truy xuất được bằng Area API hoặc không thuộc ngày khả dụng của nguồn đó, dùng luồng **Archive Download chính thức** và lưu manifest cho file archive; không giả định NRT giữ được toàn bộ dữ liệu dài hạn.
- Trước khi dùng thông tin FIRMS làm kết luận tình trạng cháy: điểm nóng **không đồng nghĩa** một vụ cháy; nhiều hotspot có thể cùng một cụm hoặc nhiều lượt quan sát vệ tinh.

**Raw:** `data/raw/nasa_firms/<source_product>/<region_id>/year=YYYY/month=MM/<request_id>.csv` + manifest. Các tệp dữ liệu được phép rỗng; parser phải nhận biết header 14 cột và lỗi API trả về HTML/text khác CSV.

## 4. OpenAQ API v3

**Chọn chỉ tiêu:** PM2.5 `pm25` / parameter ID **2** là **MUST**; PM10 `pm10` / parameter ID **1** là **SHOULD**, chỉ bật khi có coverage tốt. Không đưa NO₂/O₃/CO vào MVP v1 nếu chưa có lý do/chuẩn đơn vị (có thể là ppm/ppb).

### 4.1. Đầu vào bắt buộc khi ingestion

1. **Discovery location + sensor metadata:** API `/v3/locations` lọc Việt Nam (`iso=VN`, `parameters_id=2`) và phân trang; lưu `location.id`, `name`, `coordinates.latitude/longitude`, `country.code`, `timezone`, `provider`, `owner`, `isMonitor`, `sensors[].id`, `sensors[].parameter.{id,name,units}`, `datetimeFirst/Last`, `licenses` (nếu có). Với mỗi sensor được chọn, có thể lấy `/v3/sensors/{id}` để xác thực coverage.
2. **Historical hourly series:** `/v3/sensors/{sensor_id}/hours` với `datetime_from`, `datetime_to`, `page`, `limit`; kết quả **mỗi sensor**, không lấy latest làm dataset lịch sử.
3. **Raw measurement fields:** `value`, `parameter.name`, `parameter.units`, `period.datetimeFrom.utc`, `period.datetimeTo.utc`, `flagInfo.hasFlags`, `coverage.expectedCount`, `coverage.observedCount`, `coverage.percentComplete` (nếu có), `summary` (optional); metadata tọa độ nối theo sensor → location.

**Pilot đã truy xuất được:** Hà Nội `location_id=7441`, `sensor_id=21632`, PM2.5; dùng làm ví dụ fixture, **không khóa cứng** duy nhất trạm đó cho MVP. Danh sách location được chọn theo coverage, `license`, sensor quality và `isMonitor` sau khi khảo sát.

### 4.2. Cách truy vấn ổn định

- Sử dụng header `X-API-Key` (không in key). Khung ngày khởi đầu cho API: **24 giờ/lần/sensor**, pagination `limit=100` hoặc `limit=24`; mở rộng khi quan sát độ ổn định. Tài liệu cho phép `limit <= 1000`, nhưng không cần request tối đa trong MVP.
- Endpoint `hours` là **hourly mean**, khác nguồn `measurements` là giá trị gốc nhà cung cấp. Không gọi hourly data là raw sensor readings.
- Dùng `period.datetimeFrom.utc` làm **đầu** khoảng một giờ và `period.datetimeTo.utc` làm **cuối** khoảng, không nhầm `datetimeFrom` ở cấp top-level (mẫu thực tế đã chứng minh nó nằm trong `period`).
- Khi `HTTP 408`: giảm window/chia chunk, ghi thất bại, không retry vô hạn. Với `429`: backoff có giới hạn. Với response `meta.found` dạng `">24"`, không ép thành số nguyên exact count. Dừng phân trang theo dữ liệu và số page, không theo giả định `found` luôn là integer.
- `value=null` phải bảo toàn; `hasFlags=true` chỉ là **dấu cần xem xét**, chưa kết luận invalid. Không nhầm `coverage.percentComplete=100` với “giá trị đo hợp lệ 100%”.

**Raw metadata:** `data/raw/openaq/locations/<request_id>.json`.  
**Raw hourly:** `data/raw/openaq/hours/sensor=<id>/year=YYYY/month=MM/day=DD/page=<N>.json` + manifest/chunk-status.  
**Không push raw tùy tiện:** kiểm tra quyền sử dụng/attribution của provider trước khi đưa sample OpenAQ lên GitHub.

## 5. Hợp đồng bàn giao của Người 1 cho Người 2

Ingestion **không chuẩn hóa silver**, chỉ đảm bảo có raw file chính xác và `manifest.json` (hoặc JSONL manifest) với các trường:

| Field | Kiểu | Quy tắc |
|---|---|---|
| `schema_version` | string | `manifest-v1` |
| `source` | enum | `era5`, `nasa_firms`, `openaq` |
| `source_product` | string | `reanalysis-era5-single-levels`, `VIIRS_NOAA20_SP`, `openaq_v3_hours`... |
| `region_id` | string | mã vùng từ config; OpenAQ sensor riêng vẫn giữ region mà nó được phân bổ |
| `start_utc`, `end_utc` | ISO string | end exclusive, window **nội bộ**, không tự suy từ URL |
| `retrieved_at_utc` | ISO string | lúc nhận xong dữ liệu |
| `format` | enum | `grib`, `csv`, `json` |
| `file` | string/null | đường dẫn **relative repo**, null khi thất bại không có payload |
| `status` | enum | `success`, `empty`, `partial`, `failed` |
| `bytes` | integer/null | kích thước tệp nếu có |
| `record_count` | integer/null | null nếu chưa parse; 0 nếu parse xác nhận rỗng |
| `sha256` | string/null | checksum nếu có file |
| `request_id` | string | run/request định danh ổn định cho log |
| `error_type` | string/null | chỉ ghi mã lỗi/lý do đã sanitize, không chứa token |

**Vận hành:** không đưa secrets vào filename, URL đã log hoặc manifest. `success` nếu file hợp lệ, kiểm tra header/magic và đáp ứng kỳ vọng tối thiểu; `empty` nếu truy vấn hợp lệ trả 0 điểm/measurement; `partial` nếu một phần window/page/variable thất bại; `failed` nếu không thể lấy dữ liệu. File hỏng hoặc JSON HTML error không được gắn success.

## 6. Cấu hình công khai đề xuất (`configs/project.yaml`)

```yaml
project:
  timezone: UTC
  crs: EPSG:4326
  data_root: data
regions:
  VN_BBOX: {north: 24.0, west: 102.0, south: 8.0, east: 110.0}
  HANOI: {north: 21.25, west: 105.5, south: 20.75, east: 106.0}
ingestion:
  era5:
    dataset: reanalysis-era5-single-levels
    variables: [2m_temperature, 2m_dewpoint_temperature, 10m_u_component_of_wind, 10m_v_component_of_wind, total_precipitation]
    data_format: grib
  firms:
    historical_product_preference: VIIRS_NOAA20_SP
    recent_product: VIIRS_NOAA20_NRT
    max_days_per_request: 5
  openaq:
    api_version: v3
    parameters: [pm25]
    pilot_sensor_ids: [21632]
    max_days_per_request: 1
    page_limit: 100
integration:
  start_utc: null  # chưa xác minh window thực tế giao nhau
  end_utc: null
```

**Secrets nằm bên ngoài YAML:** ERA5 dùng `.cdsapirc` ở home; OpenAQ dùng environment variable `OPENAQ_API_KEY`; NASA dùng `FIRMS_MAP_KEY`. Không commit `.cdsapirc`, `.env`, token vào Git.

## 7. Tiêu chí nghiệm thu Ingestion v1

- [ ] ERA5 tải và giải mã được 5 biến MUST trên cùng bbox/24 giờ, ghi manifest đúng; 1–2 biến bị thiếu phải báo `partial`.
- [ ] FIRMS lấy được một mẫu hotspot lịch sử bằng sản phẩm có ngày khả dụng; phân biệt NRT/SP; xử lý đúng trường hợp CSV header-only.
- [ ] OpenAQ lấy metadata + hourly measurement đúng sensor; có phân trang, xử lý `null`, flag, timeouts.
- [ ] Cả ba tuân thủ window end-exclusive, UTC, vùng cấu hình; không hard-code key/ngày/vùng trong source code.
- [ ] Người 2 chạy được parser trên ít nhất một file raw và manifest của từng nguồn.
- [ ] Tài liệu ghi được khoảng thời gian **thật sự giao nhau**, hoặc ghi rõ nếu nguồn nào chưa đủ dữ liệu; không giả lập khả dụng.

## 8. Nguồn chính thức kiểm tra cho tài liệu này

- CDS ERA5 catalog: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
- ECMWF ERA5 accumulation/precipitation semantics: https://confluence.ecmwf.int/pages/viewpage.action?pageId=185081924
- NASA FIRMS Area API: https://firms.modaps.eosdis.nasa.gov/api/area/
- NASA FIRMS Data Availability: https://firms.modaps.eosdis.nasa.gov/api/data_availability/
- NASA FIRMS Archive Download: https://firms.modaps.eosdis.nasa.gov/download/
- OpenAQ Locations: https://docs.openaq.org/resources/locations
- OpenAQ Sensors: https://docs.openaq.org/resources/sensors
- OpenAQ Measurements/Hours: https://docs.openaq.org/resources/measurements
- OpenAQ Pagination: https://docs.openaq.org/using-the-api/pagination
- OpenAQ Timeout: https://docs.openaq.org/errors/request-timeout

**Quyết định đóng băng:** Sau khi ba thành viên phê duyệt, đóng băng phiên bản acquisition-v1 cho 3 feature branches; thay đổi nguồn/biến MUST/schema/đơn vị/window semantics phải thông báo và cập nhật contract + fixture + tests bằng PR được review.
