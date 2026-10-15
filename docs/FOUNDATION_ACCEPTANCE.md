# Foundation v1 — acceptance checklist (nhóm 3 người)

## Kiểm thử chạy được trên máy của từng thành viên

- [ ] Python 3.12.x; venv riêng; `python -m pip install -e .` thành công.
- [ ] `python main.py doctor` hiển thị `FOUNDATION PASS`.
- [ ] `python main.py mock-run` ghi file **synthetic** `data/demo/events_mock.json`.
- [ ] `python -m unittest discover -s tests -v` chạy **toàn bộ tests, OK** (v1.1 bổ sung scenario-contract checks).
- [ ] `.cdsapirc`, `.env`, secrets, `.venv`, `data/**` không hiện trong staged Git files.

## Cả 3 người phải cùng duyệt (mỗi mục chọn đồng ý hoặc mở issue)

- [ ] Phạm vi Việt Nam; 2 event types; MVP PM2.5 + NOAA-20 + 5 biến ERA5.
- [ ] Schema 6 bảng Silver/Curated và 1 JSON event; UTC, WGS84, nullable, nguồn dữ liệu.
- [ ] API raw → manifest → Silver/Curated Parquet → Event JSON; không coupling trực tiếp module.
- [ ] Synthetic fixtures chỉ dùng cho dev/tests; không dùng để chứng minh event hoặc Big Data thực.
- [ ] Quyền sửa `configs/`, `contracts/`, `main.py` thông qua Pull Request/review.
- [ ] Owner và ticket cho các mục còn mở: thời gian chồng lấp, OpenAQ QC, FIRMS archive, Spark/Java, baseline, FRP QCs.

**Khi đạt các mục trên**, commit `foundation-v1`, tạo `develop` từ `main`, rồi mỗi người tạo feature branch từ `develop` **trên clone của mình**.

**Ghi nhớ:** PASS ở giai đoạn Foundation chỉ chứng minh config/contracts/fixture/CLI skeleton hoạt động. Chưa chứng minh Spark processing, historical ingestion, anomaly detection hay production dashboard.

## Addendum v1.1: group review required before feature-branch work

- [ ] Ba thành viên đọc và duyệt `contracts/CONTRACT_DECISIONS_V1_1.md`.
- [ ] Baseline method/thresholds, FIRMS SP-versus-NRT, ERA5 context-only được thống nhất.
- [ ] Có fixture synthetic cho baseline 30 ngày, anomaly 2 giờ và fire cluster; **mới là kiểm tra hợp đồng**, không phải thuật toán production.
- [ ] Người 2 và 3 đồng ý cách định nghĩa baseline; các ngưỡng chỉ dùng thử nghiệm.
