# Contract Decisions v1.1 — Chốt giao diện & quy tắc MVP

**Dự án:** Xây dựng hệ thống thu thập, tích hợp và phân tích sự kiện môi trường từ dữ liệu quan trắc, khí tượng và viễn thám tại Việt Nam  
**Trạng thái:** **Đề xuất để ba thành viên phê duyệt** — không phải kết quả khoa học đã được xác thực.  
**Tài liệu đi kèm:** `PROJECT_CONTRACT_V1.md`, `DATA_CONTRACT_V1.md`, `DATA_ACQUISITION_V1.md` và bộ Foundation v1.  
**Mục đích:** Khóa trách nhiệm, ý nghĩa dữ liệu và giá trị *mặc định thử nghiệm* cho các thuật toán để mỗi người code song song mà không cần đợi upstream.

## 1. Chủ sở hữu và trách nhiệm cuối cùng

| Người | Sở hữu code | Quyền quyết định / bàn giao |
|---|---|---|
| **1 — Ingestion** | `src/ingestion/` | Chọn endpoint/sản phẩm và tải đúng Acquisition Contract; raw + manifest; kiểm chứng trạm, lịch sử, license, FIRMS SP/NRT; báo coverage/status thật. Không tự thay các cột Silver. |
| **2 — Processing/Spark** | `src/processing/` | Thực thi chuẩn hóa, QC, baseline, join ERA5, Silver/Curated Parquet theo Contract; phụ trách điều phối kiểm thử pipeline end-to-end trên `develop`. Không tự thay đổi cách tính baseline. |
| **3 — Event Detection + Dashboard** | `src/events/`, `src/dashboard/` | **Thiết kế yêu cầu thống kê/thuật toán và test cases**, rồi cùng Người 2 duyệt; tính anomaly/fire events và xuất `data/events/events.json`; dashboard chỉ đọc event JSON. |
| **Cả nhóm** | `contracts/`, `configs/`, `main.py`, interface chung | Quyết định thay đổi semantics, version, schema; PR được thành viên khác review. |

**Baseline specifically:** Người 3 là **algorithm owner** (định nghĩa window, min_samples, rule); Người 2 là **implementation owner** (tính bằng Spark và xuất `air_quality_baseline`); hai người cùng viết kiểm thử đối chiếu bằng fixture. Nếu khác kết quả, không merge cho đến khi thống nhất.

## 2. Vai trò ERA5 và thứ tự xử lý — CHỐT MVP

- **Chọn: ERA5 là ngữ cảnh sau khi phát hiện sự kiện**, không là feature quyết định anomaly/fire cluster ở MVP v1.
- Vẫn giữ đúng schema hiện tại: Người 2 chuẩn bị trước hai bảng enriched `air_quality_context_hourly` và `fire_hotspots_context` bằng **LEFT JOIN** với ERA5 (weather nullable). Đây là **chuẩn bị dữ liệu trước**, còn **về logic sự kiện**, Người 3 chỉ dùng PM2.5 + baseline để *phát hiện* AQ, dùng hotspot observations để *phát hiện* fire; sau đó đọc bảng context để *mô tả/enrich* event.
- Không có ERA5 hoặc join không khớp vẫn cho phép tạo event, đặt `weather_context: null`. Không dùng đồng xuất hiện để suy luận nguyên nhân.
- Temperature/gió tại validity hour `t`; **mưa của khoảng AQ `[t,t+1h)` phải lấy từ ERA5 validity `t+1h`** theo Data Contract hiện hành.

Như vậy **không cần đảo luồng, đổi schema hoặc thay toàn bộ Foundation**.

## 3. Baseline, anomaly, QC — GIÁ TRỊ THỬ NGHIỆM v1

**Đây là ngưỡng kỹ thuật cho MVP, không phải chuẩn ô nhiễm hoặc mô hình đã được kiểm chứng.** Sau khi có historical data thực, mọi điều chỉnh phải đổi `method`/`algorithm_version`, config, fixture và test.

### Baseline `same_utc_hour_median_30d_v1`

Tại mỗi observation `sensor_id, parameter=pm25, timestamp_utc=t`:

1. Lấy **30 ngày lịch sử trước t**, chỉ chọn observation **cùng giờ trong ngày theo UTC** (`hour(timestamp_utc) == hour(t)`). Không được dùng giờ hiện tại hoặc dữ liệu tương lai.
2. Chỉ dùng `value_ugm3 != null`, hữu hạn, `>= 0` và `has_flags == false`. Cờ null/unknown không vào baseline v1 (bảo tồn raw/Silver).
3. Nếu có **ít nhất 10** giá trị hợp lệ: `baseline_ugm3 = median(values)`, `sample_count = n`, `is_usable = true`, `method = "same_utc_hour_median_30d_v1"`.
4. Nếu ít hơn 10: `baseline_ugm3 = null`, `sample_count = n`, `is_usable = false`, **không gán baseline=0, không tự fallback sang công thức khác**.
5. Unique key: `(sensor_id, parameter, timestamp_utc)` và giữ schema `air_quality_baseline` hiện tại.

### `AIR_QUALITY_ANOMALY` v1

Một hourly observation là *candidate* khi `has_flags == false`, giá trị hợp lệ, baseline usable, đồng thời:

- `value_ugm3 - baseline_ugm3 >= 25` (µg/m³), **và**
- `value_ugm3 >= 1.5 * baseline_ugm3`.

Tạo một event nếu **ít nhất 2 giờ liên tiếp** trên **cùng sensor** đều là candidate. Một `null`, flagged hoặc baseline unusable sẽ ngắt chuỗi. `end_time_utc` là cuối giờ cuối (exclusive); `duration_hours` theo khoảng đầu–cuối; `metrics.baseline_ugm3` là **median các baseline của các giờ thuộc event**. Với MVP, `severity = "undetermined"` cho tới khi có rule được duyệt. Không dùng PM2.5 một giờ để tự tạo event.

**Lưu ý:** 24 measurements OpenAQ nhóm đã lấy *không đủ* để tính baseline 30 ngày; người 3 dùng synthetic fixtures đủ 30 ngày để test, người 1 sẽ khảo sát dữ liệu thật theo từng ngày và báo lỗi timeout/gap. Không được giả có anomaly thật khi chưa có baseline usable.

## 4. FIRE_HOTSPOT_CLUSTER — GIÁ TRỊ THỬ NGHIỆM v1

- **Nguồn lịch sử ưu tiên:** VIIRS NOAA-20 `SP` khi xác nhận lấy được trong giai đoạn demo; `NRT` cho ngày gần hiện tại nếu cần. Trước khi công bố tích hợp 3 nguồn, Người 1 phải kiểm chứng dữ liệu lịch sử trùng khoảng.
- Người 2 giữ **mọi quan sát raw** ở Silver; Người 3 lọc candidates chỉ với `confidence_raw ∈ {"n", "h"}` (VIIRS; chữ thường chuẩn hóa) trong test MVP. Bản ghi `l`, unknown hoặc null không trở thành candidate nhưng phải được ghi số lượng.
- **Clustering đề xuất:** ST-DBSCAN theo *cả* khoảng cách Haversine không gian **≤ 5 km** và độ lệch thời gian tuyệt đối **≤ 24 giờ**, `min_samples=2`; một cluster ít nhất 2 hotspot khác observation ID mới trở thành event. Không coi đây là 2 vụ cháy hoặc bằng chứng cháy xác thực. Trong triển khai phải test hiệu ứng nối chuỗi cluster qua nhiều ngày và version nếu bổ sung quy tắc chia cụm.
- `evidence_ids` là hotspot IDs; `metrics.hotspot_count`, `sum_frp_mw`, `max_frp_mw`, `satellites` theo Data Contract; FRP null không đổi thành 0.
- Với MVP, `severity = "undetermined"` nếu chưa đánh giá riêng.

## 5. FIRMS SP/NRT thay thế, không cộng trùng

**Quy tắc sản phẩm theo thời gian:** Đối với ngày mà `SP` đã có và đã kiểm chứng, **chọn SP làm nguồn canonical** khi xây event; không concat cả SP lẫn NRT rồi coi mỗi record là điểm nóng mới. Với các ngày chưa có SP, dùng NRT (nếu API truy cập được và ghi rõ provenance). Khi SP đến, tái xử lý ngày đó, thay NRT bằng SP, không cộng dồn kết quả.

**Cảnh báo:** `hotspot_id` hiện có `source_product` trong chuỗi hash, nên **cùng quan sát ở SP/NRT có thể có hai ID khác nhau**. Không thể chỉ gọi `dropDuplicates(["hotspot_id"])` để giải quyết trùng giữa hai sản phẩm. Cần kiểm thử lựa chọn sản phẩm ở cấp ngày và log số records bị thay thế. Không xóa Silver raw.

## 6. Dữ liệu thật, fixture và integration window

**Đã kiểm chứng truy xuất riêng:** OpenAQ Hà Nội tháng 11/2024 (24 hourly rows), FIRMS NOAA-20 NRT tháng 10/2026 (10 hotspots), ERA5 15/01/2025 (24 giờ × 2.145 grid points). **Ba mẫu này không cùng thời gian** và không thể coi là E2E 3 nguồn.

- `fixtures/raw/`: Người 1 đưa mẫu nhỏ và manifest (nếu license cho phép public), hoặc script tạo/tải sample local.
- `fixtures/silver/`: Người 2 và 3 dùng file *synthetic* đúng schema để test parser, null/QC, time alignment.
- `fixtures/curated/`: Cần **bổ sung** ít nhất một fixture synthetic của 30 ngày cùng giờ cho baseline `is_usable=true`, một fixture `is_usable=false`, 2 giờ liên tiếp anomaly, 2 hotspots gần nhau, 1 hotspot xa, 1 bản ghi thiếu weather. Fixture hiện có trong Foundation kiểm tra **cấu trúc**, chưa kiểm được logic baseline 30 ngày.
- `fixtures/events/events_mock.json`: dùng cho dashboard; dán nhãn synthetic, không dùng làm kết quả đo thật.
- **Ứng viên giai đoạn E2E**: bắt đầu khảo sát ngày `2024-11-01` UTC với OpenAQ đã xác minh, sau đó kiểm chứng FIRMS NOAA-20 **historical** và ERA5 **cùng ngày**. Đây là **ứng viên, chưa chốt** ngày chung; nếu không có FIRMS, chọn ngày khác có dữ liệu phù hợp, không tạo hotspot giả làm thật.

## 7. Dashboard v1 — chỉ đọc event JSON

Dashboard tối thiểu phải đọc `data/events/events.json` (JSON array), kể cả `[]` và `weather_context=null`; hiển thị danh sách, lọc theo loại/ngày, vị trí/centroid, thời gian bắt đầu–kết thúc, metrics theo loại event, weather context và `evidence_ids` (danh sách ID để truy vết). Không cần trực tiếp đọc raw, gọi API hay vẽ biểu đồ measurement chi tiết ở v1. Nếu muốn biểu đồ chứng cứ chi tiết, phải đề xuất một evidence lookup/output contract riêng trước khi code để tránh dashboard tự phụ thuộc bảng nội bộ. Có thể chọn Streamlit cho UI local nhưng phụ thuộc dashboard vào **event schema**, không phụ thuộc framework.

## 8. Integration owner và quy tắc merge

- **Người 2:** điều phối chạy E2E nhỏ trên `develop` và chịu trách nhiệm triage mismatch Parquet/Spark.
- **Người 1:** chứng thực raw/provenance, lỗi API và tính thật của dữ liệu.
- **Người 3:** chứng thực event JSON + dashboard, thuật toán, đánh giá trường hợp rỗng/thiếu context.
- Mỗi PR vào `develop` phải chạy `doctor`, `mock-run`, contract/unit tests, có ít nhất **1 reviewer không phải tác giả**; **không yêu cầu API live** trong CI. Thêm integration tests *fixture-driven* ở mỗi lần module thực sự sẵn sàng, và định kỳ E2E với sample thật local.
- Merge thành từng PR nhỏ; không đợi 3 feature branches hoàn thiện cuối kỳ mới ghép. Sau khi `develop` chạy E2E đạt thì mới đưa lên `main`.
- Nếu thay đổi baseline/window/schema/meaning: sửa config + docs + fixture + tests trong cùng PR và được Người 2 lẫn Người 3 cùng review.

## 9. Cấu hình đề xuất thêm vào `configs/project.yaml` (chưa tự ghi đè)

```yaml
algorithms:
  air_quality:
    baseline:
      method: same_utc_hour_median_30d_v1
      lookback_days: 30
      min_valid_samples: 10
      exclude_flagged: true
    anomaly:
      min_absolute_increase_ugm3: 25.0
      min_ratio: 1.5
      min_consecutive_hours: 2
  fire:
    algorithm: st_dbscan_v1
    spatial_radius_km: 5.0
    temporal_radius_hours: 24
    min_samples: 2
    accepted_confidence: [n, h]
    historical_preference: VIIRS_NOAA20_SP
    severity: undetermined
```

Các ngưỡng trên dùng **để viết code/test ban đầu**, phải được đánh giá độ nhạy khi có đủ dữ liệu thật, không được mô tả là tiêu chuẩn khoa học đã được kiểm chứng. Khi chưa có đủ history, hệ thống phải báo insufficient coverage thay vì tạo event giả.

## 10. Danh sách duyệt nhóm trước khi tách nhánh

- [ ] Người 1 chấp nhận Acquisition/manifest, chính sách NRT/SP và trách nhiệm data coverage.
- [ ] Người 2 chấp nhận baseline `same_utc_hour_median_30d_v1`, Spark implementation, curated schema và E2E ownership.
- [ ] Người 3 chấp nhận candidate/anomaly/ST-DBSCAN rules, fixtures và event dashboard schema.
- [ ] Cả ba hiểu các ngưỡng này là **MVP defaults**; dữ liệu thật, chất lượng detection và Big Data benchmark vẫn phải được xác minh.
- [ ] Nhóm approve Addendum v1.1 bằng Pull Request, rồi bắt đầu phát triển song song.
