# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Vũ Tiến Linh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602657` |
| GitHub | `Linh-Huong` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07900` |

## Kết quả cuối

- `dataset_version` của bạn: `2c2f205f5c69`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `train: 667 / val: 176 / test: 176 (tổng: 1019 khung hình)`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt chính giữa telemetry nguồn A và nguồn B được ghi nhận ở Bronze:
1. **Tên cột**: Nguồn A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `gas`, `brake`, `steer`. Nguồn B đặt tên khác hoàn toàn: `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `altitude`, `throttle_pct`, `brake_pct`, `steering`.
2. **Đơn vị đo**: Tốc độ nguồn A đo bằng mét/giây (`m/s`), bàn đạp ga/phanh theo khoảng `[0.0, 1.0]`. Nguồn B đo tốc độ bằng `km/h`, bàn đạp đo theo phần trăm `0 - 100%`.
3. **Định dạng thời gian**: Nguồn A dùng chuỗi chuẩn ISO-8601 UTC (`2025-10-07T08:00:00.014Z`), trong khi nguồn B dùng Unix epoch timestamp tính bằng mili-giây (`1759933801509`).
4. **Độ lệch đồng hồ (Clock Drift)**: Nguồn B bị lệch đồng hồ 1500 ms (1.5 giây) so với trục thời gian video, trong khi nguồn A đồng bộ trực tiếp. Ngoài ra, số lượng dòng thô cũng khác nhau (A có 3021 dòng, B có 3234 dòng).

**Lý do giữ mỗi nguồn một bảng và để mọi cột dạng chữ:**
- Tầng Bronze tuân thủ nguyên tắc Medallion (ghi nguyên bản, append-only): dữ liệu từ nhà cung cấp/thiết bị phải được lưu lại trung thực 100% để làm nguồn chân lý duy nhất (Single Source of Truth).
- Tránh sập pipeline do Schema Drift: Nếu ép kiểu ngay tại Ingestion, một giá trị rác, ô trống hay lỗi định dạng sẽ làm gãy pipeline và mất dữ liệu. Lưu dạng chuỗi đảm bảo quá trình nhận dữ liệu luôn thành công (land safely).
- Tách bạch trách nhiệm (Separation of Concerns): Tầng Bronze tập trung lưu trữ an toàn, còn việc chuẩn hóa schema, ép kiểu và xử lý chất lượng được chuyển sang tầng Silver (thực hiện bằng dbt và DuckDB). Điều này cho phép dễ dàng re-process/replay dữ liệu bất cứ lúc nào mà không cần thu thập lại từ nguồn.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Cách pipeline đưa hai nguồn về một schema thống nhất:**
- **Đơn vị đo**: Nguồn B đo vận tốc bằng `km/h` được quy đổi về `m/s` theo công thức `round(cast(spd_kmh as double) / 3.6, 3)`. Bàn đạp ga/phanh ở dạng phần trăm (`throttle_pct`, `brake_pct`) được chia cho 100 để về khoảng `[0.0, 1.0]`.
- **Kiểu thời gian**: Nguồn A parse chuỗi ISO-8601 sang timestamp UTC. Nguồn B chuyển đổi từ epoch mili-giây sang timestamp UTC bằng hàm `epoch_ms(cast(timestamp_ms as bigint))`.
- **Độ lệch đồng hồ (Clock Offset)**: Nguồn B khai báo bị lệch 1500 ms (1.5 giây) trong `sources.json`. Pipeline trừ độ lệch này: `ts - interval '1500 millisecond'` để khớp chính xác với mốc thời gian của camera video.

**Bằng chứng đã xử lý đúng:**
- Thời điểm bắt đầu của telemetry B (`min(ts)`) sau chuẩn hóa là `2025-10-08 14:30:00.009000+00:00`, trùng khớp hoàn toàn với thời điểm bắt đầu của video B (`2025-10-08 14:30:00`). Test tự động `drive B clock offset removed` đã đạt PASS.
- Tốc độ tối đa sau khi quy đổi rất hợp lý: Drive A đạt tối đa 71 km/h (19.8 m/s), Drive B đạt tối đa 100 km/h (27.8 m/s).

**Bảng quarantine (dữ liệu cách ly):**

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng số dòng bị cách ly là 53 dòng mỗi drive (tổng 106 dòng). Số dòng sạch thu được: Drive A là 2968 dòng, Drive B là 3181 dòng. Tổng dòng sạch + cách ly đúng bằng tổng dòng nhận về từ Bronze (A: 2968 + 53 = 3021; B: 3181 + 53 = 3234).

**Vì sao dòng lỗi vào `rejects` thay vì bị xóa:**
- **Tránh mất mát dữ liệu âm thầm (Silent data loss)**: Xóa dòng lỗi làm mất khả năng kiểm toán (audit) và đối soát tổng số bản ghi giữa các tầng.
- **Phục vụ phân tích nguyên nhân gốc (Root cause analysis)**: Lưu giữ các dòng lỗi kèm lý do (`reject_reason`) giúp kỹ sư dữ liệu phát hiện lỗi phần cứng, cảm biến mất kết nối hoặc firmware truyền sai định dạng.
- **Bảo toàn dữ liệu (Data Lineage)**: Giữ nguyên vẹn quan hệ "Dữ liệu nhận = Dữ liệu sạch + Dữ liệu cách ly", cho phép sửa luật và re-process khi cần mà không mất mát thông tin.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**Phân biệt `warn` và `error`:**
- **`in_range` là `error` (dừng pipeline)**: Tốc độ xe ô tô thực tế bị giới hạn bởi quy luật vật lý (trong khoảng 0 đến 70 m/s, tương đương 0 - 252 km/h). Giá trị âm hoặc vượt quá 70 m/s chắc chắn là dữ liệu hỏng. Nếu đưa vào tầng Gold, mô hình AI lái xe tự động sẽ học sai quy luật vật lý và gây nguy hiểm chết người. Do đó vi phạm tính toàn vẹn này bắt buộc phải dừng pipeline ngay lập tức (`error`).
- **`max_gap_ms` là `warn` (cảnh báo nhưng cho đi tiếp)**: Việc cảm biến bị trễ hoặc mất tín hiệu tạm thời (ví dụ xe chui vào hầm, bóng râm, mất sóng GPS trong vài giây) là hiện tượng vật lý bình thường khi vận hành thực tế ngoài đường. Cần gắn cảnh báo để theo dõi chất lượng kết nối, nhưng không thể vì mất tín hiệu vài giây mà huỷ bỏ toàn bộ dữ liệu hành trình xe (`warn`).

**Kết quả thí nghiệm `break-gate` (`break_gate.json`):**
- Khi nới lỏng luật từ 70 m/s lên 1000 m/s trong bước staging/silver, các giá trị tốc độ vô lý đã lọt vào bảng `silver_telemetry`.
- Cổng chất lượng dbt test đã chặn lại thành công: Test `in_range_silver_telemetry_speed_mps__70__0` bị **FAIL** và bắt được chính xác **6 dòng** vi phạm (mỗi drive 3 dòng tốc độ dị biệt).
- Kết quả test: `PASS=5 WARN=1 ERROR=1 SKIP=0`, pipeline dừng lại đúng như thiết kế, ngăn không cho dữ liệu bẩn lọt xuống downstream. Sau khi khôi phục luật, test trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`.

**Vai trò của "cổng chất lượng" (Quality Gate):**
- Là lá chắn tự động (automated guardrail) đảm bảo chỉ những dữ liệu đạt chuẩn độ tin cậy mới được nạp vào Gold để huấn luyện mô hình.
- Giúp phát hiện sai sót sớm ngay tại ranh giới tầng dữ liệu (fail-fast), giảm thiểu chi phí khắc phục lỗi và ngăn chặn thảm họa suy luận sai lệch ở tầng AI/Robotics.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**Số liệu từ `idempotency.json` (kiểm tra trên partition `trip-00003` có 60 khung):**
- **Trước khi chạy lại**: Bảng có 1246 dòng, dấu vân tay (fingerprint) là `6c7480dfde7e`.
- **Sau 2 lần chạy lại liên tiếp**: Bảng vẫn giữ đúng **1246 dòng**, dấu vân tay vẫn là `6c7480dfde7e` (hoàn toàn không đổi, `unchanged: true`).
- **Khi ghi kiểu `append` (cách làm sai)**: Sau 2 lần chạy, dữ liệu của trip bị nhân đôi lên **120 dòng** (tăng gấp đôi `2x`), làm sai lệch toàn bộ tập dữ liệu.

**Vì sao Delta `overwrite` theo partition an toàn hơn "xoá rồi ghi lại" (Delete then Insert):**
- **Tính nguyên tử (ACID Transaction)**: Trong Delta Lake, phép overwrite theo predicate partition (`trip_id = '...'`) được thực hiện như một giao dịch nguyên tử. Dữ liệu mới được ghi vào các file Parquet mới, và chỉ khi toàn bộ quá trình thành công thì Delta Transaction Log mới cập nhật con trỏ metadata thay thế các file cũ.
- **An toàn khi gặp lỗi giữa chừng**: Nếu tiến trình bị sập (crash, mất điện, tràn đĩa) giữa chừng, trạng thái cũ của partition vẫn còn nguyên vẹn 100%, Delta Lake tự động dọn dẹp các file rác chưa commit.
- **Nguy cơ của "xóa rồi ghi lại"**: Nếu thực hiện xoá rồi ghi (2 thao tác độc lập), khi xảy ra lỗi sau bước xóa, partition sẽ bị mất trắng dữ liệu (data loss). Ngoài ra, các tiến trình đọc đồng thời (concurrent readers) trong lúc đó sẽ đọc phải partition rỗng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Sự khác nhau giữa khung đen và khung tối:**
- **Khung đen (`is_black`)**: Do sự cố thiết bị (ống kính bị che, cảm biến mất nguồn, cáp hỏng), toàn bộ ma trận điểm ảnh có độ sáng xấp xỉ 0, hoàn toàn không chứa thông tin đặc trưng thị giác. Pipeline đánh cờ `is_black` và **loại bỏ hoàn toàn** khỏi tập Gold.
- **Khung tối (`is_dark`)**: Do bối cảnh môi trường thực tế (xe di chuyển trong hầm, bóng râm cây cối, ánh sáng hoàng hôn). Khung hình tuy thiếu sáng nhưng vẫn chứa đầy đủ cấu trúc hình học, vạch kẻ đường, đèn xe phía trước. Pipeline đánh cờ `is_dark` nhưng **giữ lại** (`toi_giu_lai` ở Drive B giữ lại tới 61 khung).
- **Quan điểm**: Tôi hoàn toàn đồng ý. Xe tự lái bắt buộc phải học cách nhận diện và phản xạ trong điều kiện ánh sáng yếu/đường hầm. Nếu loại bỏ khung tối, mô hình sẽ bị "mù" khi vận hành ban đêm hoặc trong hầm.

**Số lượng khung trùng theo ngưỡng dHash (từ `threshold.json`):**

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**Lựa chọn ngưỡng và sự phụ thuộc vào tốc độ xe:**
- **Ngưỡng chọn**: Tôi chọn ngưỡng **2 bit** (ngưỡng mặc định của pipeline). Ngưỡng 0 bit quá khắt khe (chỉ bắt được ảnh giống hệt từng bit), bỏ sót khung hình bị nhiễu sensor nhẹ khi xe dừng. Ngưỡng 6 bit lại quá nới lỏng, loại bỏ tới hơn 50% số khung hình (363/602 ở A và 244/644 ở B), làm mất cả khung hình có chuyển động thật. Ngưỡng 2 bit là điểm cân bằng lý tưởng, bắt trọn 6 giây camera đứng hình ở Drive A mà không làm mất thông tin động.
- **Phụ thuộc vào tốc độ xe**: Tốc độ xe càng thấp (như Drive A chạy chậm ~39 km/h, có đoạn dừng chờ), khung hình liên tiếp chụp cách nhau 0.5s rất ít thay đổi, khoảng cách Hamming của dHash rất nhỏ dẫn đến số khung trùng cao (149 khung ở ngưỡng 2). Ngược lại khi xe chạy nhanh (Drive B chạy 65 - 93 km/h), cảnh quan dịch chuyển liên tục nên dHash biến đổi lớn, số khung trùng ít hơn hẳn (chỉ 53 khung ở ngưỡng 2).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
