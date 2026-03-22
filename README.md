# Oracle Sync — Công cụ đồng bộ dữ liệu Oracle → Oracle

Ứng dụng web Flask cho phép khai báo kết nối, định nghĩa mapping cột và thực thi đồng bộ dữ liệu giữa hai database Oracle. Toàn bộ cấu hình lưu trong file (XML/CSV), không cần bảng trong database.

---

## Mục lục

1. [Cấu trúc dự án](#cấu-trúc-dự-án)
2. [Cách chạy](#cách-chạy)
3. [Luồng nghiệp vụ chi tiết](#luồng-nghiệp-vụ-chi-tiết)
   - [B1. Sửa kết nối](#b1-sửa-kết-nối)
   - [B2. Chạy job delete](#b2-chạy-job-delete)
   - [B3. Chạy các job Danh sách bảng cần đồng bộ](#b3-chạy-các-job-danh-sách-bảng-cần-đồng-bộ)
   - [B4. Chạy tiếp danh sách script](#b4-chạy-tiếp-danh-sách-script)
4. [Lưu trữ dữ liệu](#lưu-trữ-dữ-liệu)
5. [API endpoints](#api-endpoints)
6. [Docker](#docker)

---

## Cấu trúc dự án

```
Clone_data/
├── config/
│   ├── file_config.py        # Đọc/ghi sync_config.xml (kết nối Oracle)
│   ├── csv_statements.py     # Đọc/ghi job_sync.csv (danh sách job)
│   ├── xml_history.py        # Đọc/ghi job_his.xml (lịch sử chạy job)
│   ├── xml_deleted.py        # Đọc/ghi job_delete_his.xml (lịch sử DELETE)
│   └── xml_script_history.py # Đọc/ghi script_his.xml (lịch sử chạy script)
│
├── src/
│   ├── metadata_loader.py    # Truy vấn user_tables / user_tab_columns từ Oracle
│   └── config_loader.py      # Adapter đọc kết nối (dùng cho run_sync legacy)
│
├── scripts/                  # *** Data files — mount volume Docker ***
│   ├── sync_config.xml       # Danh sách kết nối Oracle
│   ├── job_sync.csv          # Danh sách job đồng bộ + câu lệnh SQL
│   ├── job_his.xml           # Lịch sử chạy từng job
│   ├── job_delete_his.xml    # Lịch sử câu lệnh DELETE đã thực thi
│   ├── script_his.xml        # Lịch sử chạy script .sql
│   ├── sql_notes.json        # Ghi chú theo tên file script
│   └── sql/                  # Thư mục script .sql (scripts/sql/*.sql)
│
├── web/
│   ├── app.py                # Flask application — routes + business logic
│   ├── static/css/main.css   # Stylesheet
│   └── templates/
│       ├── _menu.html              # Navigation sidebar
│       ├── index.html              # Danh sách job (trang chủ)
│       ├── job_new.html            # Form tạo job mới
│       ├── man_hinh_them_moi.html  # Mapping cột + sinh SQL
│       ├── connection.html         # Quản lý kết nối
│       ├── connection_edit.html    # Form sửa kết nối
│       ├── job_errors.html         # Danh sách job lỗi gần nhất
│       ├── job_history.html        # Lịch sử chạy của 1 job
│       ├── job_stats_success.html  # Thống kê job chạy thành công
│       ├── job_deleted_history.html# Lịch sử câu lệnh DELETE
│       ├── run_result.html         # Kết quả chạy (non-SSE)
│       ├── scripts_add.html       # Thêm/quản lý file .sql
│       ├── scripts_run.html       # Chạy script .sql
│       ├── run_delete_scripts.html # Chạy JOB_DELETE_*.sql
│       └── script_history.html    # Lịch sử chạy script
│
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Cách chạy

### Chạy trực tiếp (Python)

```bash
# Cài dependencies
pip install -r requirements.txt

# Chạy từ thư mục gốc (Clone_data)
PYTHONPATH=. python web/app.py

# Windows (PowerShell)
$env:PYTHONPATH="."; python web/app.py
```

Truy cập: `http://localhost:5000`

---

### Chạy bằng Docker

> **Yêu cầu:** Docker Desktop đang chạy (icon taskbar chuyển xanh).  
> Kiểm tra: `docker info` — nếu in thông tin engine là Docker đang chạy.

#### Bước 1 — Build image

```powershell
cd D:\Clone_data
docker compose build
```

> Image được build từ `Dockerfile`. Bước này chỉ cần chạy lại khi thay đổi source code.

#### Bước 2 — Khởi động container

```powershell
docker compose up -d
```

> Khi container start, `entrypoint.sh` tự động:
> 1. Reset `scripts/job_his.xml` về trạng thái rỗng `<histories />`
> 2. Khởi động Flask tại cổng 5000

Truy cập: `http://localhost:5000`

#### Kiểm tra log

```powershell
docker compose logs -f oracle-sync
```

#### Dừng container

```powershell
docker compose down
```

#### Rebuild sau khi sửa code

```powershell
docker compose build
docker compose up -d
```

> **Lưu ý:** Thư mục `scripts/` được mount làm volume — `sync_config.xml`, `job_sync.csv`  
> **không bị mất** khi rebuild image. Chỉ `job_his.xml` (lịch sử chạy) được reset mỗi lần start.

---

## Luồng nghiệp vụ chi tiết

### B1. Sửa kết nối

**Màn hình:** `Cấu hình → Kết nối`  
**File lưu:** `scripts/sync_config.xml`

**Điền đúng địa chỉ IP nguồn và đích** (Host, Port, Service name, Username, Password) cho từng kết nối.

```
Người dùng sửa: Host (IP nguồn/đích), Port, Service name, Username, Password
       ↓
POST /connection/<id>/update
       ↓
file_config.py: update_connection()
  → Ghi vào sync_config.xml
```

Thêm mới: `POST /connection/add` — Kiểm tra tên không trùng, gán ID tự tăng.

---

### B2. Chạy job delete

**Màn hình:** `Script → Chạy JOB_DELETE_PN_LAI_LICHS`  
**Thư mục:** `scripts/sql/JOB_DELETE_*.sql`

```
Người dùng chọn: Script JOB_DELETE_* + Kết nối (target)
Bấm "Chạy đã chọn"
       ↓
POST /api/scripts/run-one (async với run_id)
  → Chạy tuần tự từng script
  → Poll /api/scripts/run-status/<run_id>
  → Có thể bấm "Dừng" → POST /api/scripts/stop/<run_id>
```

---

### B3. Chạy các job Danh sách bảng cần đồng bộ

**Màn hình:** `Đồng bộ → Danh sách job` (trang chủ)

Chọn job cần chạy → Bấm "Chạy đã chọn" → Chọn số luồng (1–4).

#### Luồng SSE (giao diện chính)

```
Người dùng chọn checkbox các job cần chạy
Bấm "Chạy đã chọn"
       ↓
Dialog chọn số luồng song song (1–4)
       ↓
JavaScript mở modal tiến độ + kết nối SSE:
  GET /api/jobs/run-stream?ids=1,2,3&workers=4
       ↓
app.py: jobs_run_stream()
  - Tạo run_id (uuid), stop_event, conns_map (thread-safe)
  - Đẩy event "started" → client nhận run_id

  ThreadPoolExecutor(max_workers=N):
    Mỗi job chạy trong thread riêng (_run_one):

    ┌──────────────────────────────────────┐
    │ _run_one(idx, stmt_id, ...)          │
    │                                      │
    │ 1. Kiểm tra stop_event               │
    │ 2. Đọc thông tin job từ job_sync.csv │
    │ 3. Push event "running" → modal      │
    │ 4. Validate (bảng đích, sql, kết nối)│
    │ 5. Kết nối Oracle target             │
    │    → Lưu conn vào conns_map          │
│ 6. DELETE FROM target_conn.table     │
│    → commit()                        │
│    → add_delete_op() → job_delete_his.xml
│ 7. _execute_insert():                │
    │    a. COUNT(*) source rows           │
    │    b. Nếu > LARGE_TABLE_THRESHOLD:   │
    │       → INSERT /*+ APPEND */ INTO... │
    │       → Push event "chunk" (⚡)      │
    │    c. Nếu <= ngưỡng:                 │
    │       → INSERT thông thường          │
    │    → commit()                        │
    │ 8. Ghi lịch sử vào job_his.xml      │
    │    (dùng _history_lock)              │
    │ 9. Push event "progress" (success)   │
    └──────────────────────────────────────┘

  Generator đọc từ Queue → yield SSE events → client
  Khi tất cả job xong: push event "done"
```

**INSERT /*+ APPEND */ (Oracle Direct-Path Write):**
- Áp dụng khi số dòng nguồn > `LARGE_TABLE_THRESHOLD` (mặc định 100,000)
- Oracle ghi thẳng vào datafile, bỏ qua buffer cache
- Nhanh hơn 3–10× so với INSERT thông thường cho bảng lớn
- Yêu cầu: COMMIT DELETE trước, COMMIT INSERT sau (không thể trong cùng transaction)

#### Dừng job đang chạy

```
Người dùng bấm "Dừng"
       ↓
POST /api/jobs/stop/<run_id>
       ↓
app.py: jobs_stop()
  - Đặt stop_event = True
  - Duyệt conns_map → gọi oc.cancel() trên mỗi Oracle connection
    (Oracle ngắt ngay câu lệnh đang blocking)
  - Các thread chưa bắt đầu → kiểm tra stop_event → bỏ qua
```

#### Chạy tiếp (resume)

Sau khi dừng hoặc có lỗi, nút **"Chạy tiếp"** xuất hiện:
- Chỉ chạy lại các job **chưa hoàn thành** (loại trừ job đã success hoặc error)
- Giữ nguyên số luồng của lần chạy trước

---

### B4. Chạy tiếp danh sách script

**Màn hình:** `Script → Chạy script .sql`  
**Thư mục:** `scripts/sql/*.sql` (loại trừ `JOB_DELETE_*.sql` — có menu riêng B2)

#### Luồng chạy script (async với run_id, poll status, stop)

```
Người dùng chọn: Script + Kết nối (target) cho từng dòng
Bấm "Chạy đã chọn"
       ↓
Client gọi runScriptsSequential(pairs, 0):
  Với mỗi script (chạy tuần tự):

  1. Tạo run_id = 'script_' + Date.now() + '_' + index
  2. POST /api/scripts/run-one
     body: filename, connection_id, run_id
       ↓
  app.py: api_script_run_one()
    - Nếu có run_id:
      → Tạo stop_event, state dict
      → _script_runs[run_id] = state
      → Thread spawn: _script_run_worker(run_id, filename, conn_id)
      → Trả về ngay: { run_id, status: "running" }
    - Nếu không có run_id: chạy sync như cũ

  3. Client nhận run_id → pollScriptStatus(run_id, pairs, index);
     GET /api/scripts/run-status/<run_id> mỗi 200ms
       ↓
  app.py: api_script_run_status()
    - Trả về status: "running" | "success" | "error" | "stopped"
    - Khi xong: pop khỏi _script_runs, trả về result

  4. Nếu status = "running" → tiếp tục poll
  5. Nếu status = "success" → chạy script tiếp theo (index+1)
  6. Nếu status = "error" → redirect với thông báo lỗi
  Khi tất cả xong: redirect với message thành công
```

#### Worker chạy script (_run_one_script_with_stop)

```
_script_run_worker(run_id, filename, conn_id):
       ↓
_run_one_script_with_stop(filename, conn_id, stop_event):
  1. Đọc file scripts/sql/<filename>
  2. Parse SQL: split theo ";", bỏ dòng trống và comment (--)
  3. Kết nối Oracle target (theo conn_id)
  4. Với mỗi câu lệnh trong statements:
     - Kiểm tra stop_event.is_set()
       → Nếu set: rollback, return (filename, "Đã dừng theo yêu cầu.")
     - cur.execute(stmt)
  5. commit
  6. add_script_run(filename, conn_id, conn_name, status, message)
  7. Cập nhật state["status"], state["result"]
```

#### Dừng script đang chạy

```
Người dùng bấm "Dừng"
       ↓
POST /api/scripts/stop/<run_id>
       ↓
app.py: api_script_stop()
  - state["stop"].set()
  - Worker kiểm tra stop_event trước mỗi câu lệnh → thoát sớm
```

#### Chạy batch (form submit)

```
POST /scripts/run (form submit, nhiều script)
  → Chạy tuần tự sync (không dùng run_id)
  → _run_one_script() cho từng script
  → Nếu lỗi: dừng ngay, redirect với message
  → redirect_to: /scripts hoặc /scripts/run-delete-pn-lai-lichs
```

#### Các chức năng khác

- **Tạo mới:** `POST /api/scripts/new` — tạo file .sql trong scripts/sql
- **Đọc:** `GET /api/scripts/<filename>` — nội dung file
- **Lưu:** `POST /api/scripts/<filename>` — ghi nội dung
- **Xóa:** `DELETE /api/scripts/<filename>` — xóa file
- **Ghi chú:** `POST /api/scripts/note` — lưu note vào scripts/sql_notes.json

---

## Lưu trữ dữ liệu

| File | Loại | Nội dung |
|---|---|---|
| `scripts/sync_config.xml` | XML | Danh sách kết nối Oracle (host, port, user, pass) |
| `scripts/job_sync.csv` | CSV | Danh sách job: id, name, kết nối nguồn/đích, bảng, sql_text |
| `scripts/job_his.xml` | XML | Lịch sử mỗi lần chạy job |
| `scripts/job_delete_his.xml` | XML | Lịch sử câu lệnh DELETE đã thực thi |
| `scripts/script_his.xml` | XML | Lịch sử chạy script .sql |
| `scripts/sql_notes.json` | JSON | Ghi chú theo tên file script |

**Không có database trung gian** — tất cả config và lịch sử lưu trong file.

### Cấu trúc job_sync.csv

```
id,name,source_connection_name,target_connection_name,source_table,target_table,sql_text
77,JOB_DM_MA_THOI_GIANS,QLPN_OLD,QLPN,MA_MUC_THOI_GIAN,DM_MA_THOI_GIANS,"INSERT INTO QLPN.DM_MA_THOI_GIANS (...) SELECT ... FROM QLPN_OLD.MA_MUC_THOI_GIAN"
```

---

## API Endpoints

| Method | URL | Mô tả |
|---|---|---|
| GET | `/` | Danh sách job |
| GET | `/connection` | Quản lý kết nối |
| POST | `/connection/add` | Thêm kết nối |
| GET | `/connection/<id>/edit` | Form sửa kết nối |
| POST | `/connection/<id>/update` | Cập nhật kết nối |
| POST | `/connection/<id>/delete` | Xóa kết nối |
| GET | `/api/connections/<id>/tables` | Lấy danh sách bảng từ Oracle |
| GET | `/api/connections/<id>/columns` | Lấy danh sách cột của bảng |
| GET | `/job/new` | Form tạo job mới |
| POST | `/job/create` | Tạo job + redirect sang mapping |
| GET | `/job/<id>/them-moi` | Màn hình mapping cột |
| POST | `/job/<id>/save` | Lưu câu lệnh SQL |
| POST | `/api/job/<id>/sql` | Lưu SQL nhanh (inline edit) |
| POST | `/job/<id>/delete` | Xóa 1 job |
| POST | `/jobs/delete-bulk` | Xóa nhiều job |
| GET | `/api/jobs/run-stream` | Chạy job (SSE real-time, đa luồng) |
| POST | `/api/jobs/stop/<run_id>` | Dừng run job đang chạy |
| GET | `/jobs/errors` | Danh sách job lỗi gần nhất |
| GET | `/jobs/stats-success` | Thống kê job chạy thành công |
| GET | `/jobs/deleted-history` | Lịch sử câu lệnh DELETE |
| GET | `/job/<id>/history` | Lịch sử chạy của 1 job |
| GET | `/scripts` | Chạy script .sql |
| POST | `/scripts/run` | Chạy script (form batch, sync) |
| GET | `/scripts/history` | Lịch sử chạy script |
| GET | `/scripts/run-delete-pn-lai-lichs` | Chạy JOB_DELETE_*.sql |
| POST | `/api/scripts/new` | Tạo file script mới |
| GET | `/api/scripts/<filename>` | Đọc nội dung file script |
| POST | `/api/scripts/<filename>` | Lưu nội dung file script |
| DELETE | `/api/scripts/<filename>` | Xóa file script |
| POST | `/api/scripts/run-one` | Chạy 1 script (async nếu có run_id) |
| GET | `/api/scripts/run-status/<run_id>` | Lấy trạng thái chạy script |
| POST | `/api/scripts/stop/<run_id>` | Dừng script đang chạy |
| POST | `/api/scripts/note` | Lưu ghi chú cho file script |

---

## Docker

### Các lệnh thường dùng

| Mục đích | Lệnh |
|---|---|
| Kiểm tra Docker đang chạy | `docker info` |
| Build image | `docker compose build` |
| Start container | `docker compose up -d` |
| Build + Start (lần đầu hoặc sau khi sửa code) | `docker compose build && docker compose up -d` |
| Xem log realtime | `docker compose logs -f oracle-sync` |
| Dừng container | `docker compose down` |
| Xem trạng thái container | `docker compose ps` |

### Hành vi khi container start

Mỗi lần `docker compose up -d`, `entrypoint.sh` tự động chạy:
```
[init] Da reset job_his.xml   ← xóa lịch sử chạy cũ
[start] Khoi dong Flask...    ← Flask lắng nghe port 5000
```

### Volume và data

```
Host (D:\Clone_data\scripts\)  ←→  Container (/app/scripts/)
```

| File | Xử lý khi start |
|---|---|
| `sync_config.xml` | Giữ nguyên (kết nối Oracle) |
| `job_sync.csv` | Giữ nguyên (danh sách job) |
| `job_his.xml` | **Reset về rỗng** mỗi lần start |

> **Lưu ý:** Docker Desktop phải đang chạy (icon taskbar xanh) trước khi dùng lệnh docker.
