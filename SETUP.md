# mongo-pg-sync — Docker hoá cho production

Đóng gói `sync-mongo-to-pg.js` (đồng bộ MongoDB → PostgreSQL, batch-commit,
worker_threads) vào Docker, có giới hạn tài nguyên rõ ràng để **không**
ảnh hưởng tới các service khác trên cùng server hoặc tới MongoDB/PostgreSQL
production khi chạy.

## Vì sao bản gốc có thể "ảnh hưởng cả server"

Chạy trực tiếp `node sync-mongo-to-pg.js` trên host, ba thứ **không có trần**:

1. **`WORKER_COUNT` mặc định = 10** worker thread chạy song song, mỗi worker
   tự chạy nguyên bộ aggregation pipeline (nhiều `$lookup`) — 10 tiến trình
   CPU-bound cùng lúc, không giới hạn CPU nào chặn lại.
2. **Mỗi worker tự mở 1 connection pool riêng** tới MongoDB và Postgres.
   Driver `mongodb` mặc định `maxPoolSize=100` nếu không set — 10 worker ×
   pool mặc định có thể tạo ra **hàng trăm connection** tới MongoDB cùng lúc.
   Tương tự `pg.Pool` mặc định `max=10` → tới ~100 connection Postgres.
3. **Không có giới hạn bộ nhớ/CPU ở mức tiến trình** — nếu 1 batch bất
   thường lớn hoặc pipeline chạy chậm do thiếu index, Node có thể ngốn RAM
   không kiểm soát, kéo cả host chậm theo (kể cả các service khác đang chạy
   trên host đó).

Việc thiếu index cho `$lookup` (script đã tự cảnh báo qua
`MONGO_IDLE_TIMEOUT_SECS`) làm vấn đề trầm trọng hơn, nhưng **ngay cả khi có
đủ index**, số lượng connection/CPU không giới hạn vẫn là rủi ro độc lập.

## Đã thay đổi gì

**Trong `sync-mongo-to-pg.js`:**
- `WORKER_COUNT` mặc định hạ từ `10` xuống `4`.
- Thêm `PG_POOL_MAX` (mặc định `2`) — giới hạn connection Postgres/worker.
- Thêm `MONGO_MAX_POOL_SIZE` / `MONGO_MIN_POOL_SIZE` (mặc định `5` / `0`) —
  giới hạn connection MongoDB/worker (thay vì mặc định driver là 100).
- Thêm `MONGO_READ_PREFERENCE` (tuỳ chọn) — có thể đọc từ secondary để giảm
  tải primary.
- Thêm `BATCH_DELAY_MS` (mặc định `0`) — nghỉ giữa các batch nếu cần "nhẹ
  tay" hơn với Postgres production.
- Thêm `WORKER_MAX_OLD_GEN_MB` (mặc định `512`) — giới hạn heap V8 của từng
  worker thread qua `resourceLimits`, độc lập với giới hạn Docker.

Toàn bộ logic nghiệp vụ (pipeline, batch-commit, retry, idle-watchdog,
graceful shutdown...) giữ nguyên — chỉ thêm trần tài nguyên, không đổi hành
vi khi các biến mới để mặc định gần với hành vi cũ (trừ `WORKER_COUNT`).

**File mới (Docker hoá):**
- `Dockerfile` — multi-stage, image `node:20-alpine`, chạy bằng user
  non-root, `tini` làm PID 1 để forward tín hiệu dừng đúng cách, giới hạn
  heap tiến trình master qua `NODE_OPTIONS`.
- `docker-compose.yml` — giới hạn `mem_limit` / `cpus` / `pids_limit` ở
  mức container, giới hạn log trên disk, `restart: "no"` (đây là job chạy
  xong rồi thoát, không phải service chạy mãi).
- `.env.example` — liệt kê toàn bộ biến môi trường kèm giải thích.
- `.dockerignore`, `.gitignore`.

## Chạy thử

```bash
cp .env.example .env
# sửa .env: MONGO_URI, PG_HOST/PG_DB/PG_USER/PGPASSWORD cho đúng môi trường

# build image
docker compose build

# test nhanh với it dong du lieu truoc khi chay full (an toan, khong sua PG_TABLE that)
# sua trong .env: LIMIT_DOCS=50
docker compose run --rm sync

# xem log realtime neu chay o che do khac
docker compose logs -f sync
```

Vì đây là job batch (chạy xong tự thoát), dùng `docker compose run --rm sync`
để chạy 1 lần — **không** dùng `docker compose up -d` (compose sẽ không tự
lặp lại job, nhưng `up -d` phù hợp hơn cho service chạy mãi; `run --rm` rõ
ràng hơn cho ý định "chạy 1 lần rồi dọn container").

## Chạy định kỳ trên production

Script tự thoát sau khi đồng bộ xong 1 lượt — lên lịch lặp lại bằng cron
hoặc systemd timer ở tầng host, KHÔNG dùng `restart: always` trong compose
(nếu không sẽ thành vòng lặp sync liên tục 24/7 vào DB production).

**Cron** (ví dụ chạy mỗi giờ, log ra file riêng có xoay vòng theo ngày):

```cron
0 * * * * cd /path/to/mongo-pg-sync && /usr/bin/docker compose run --rm sync >> /var/log/mongo-pg-sync/$(date +\%F).log 2>&1
```

**systemd timer** (khuyến nghị hơn cron cho production — có log qua
journald, dễ theo dõi trạng thái lần chạy gần nhất):

`/etc/systemd/system/mongo-pg-sync.service`
```ini
[Unit]
Description=Dong bo MongoDB -> PostgreSQL (mongo-pg-sync)
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
WorkingDirectory=/path/to/mongo-pg-sync
ExecStart=/usr/bin/docker compose run --rm sync
```

`/etc/systemd/system/mongo-pg-sync.timer`
```ini
[Unit]
Description=Chay mongo-pg-sync moi gio

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now mongo-pg-sync.timer
sudo systemctl status mongo-pg-sync.timer
journalctl -u mongo-pg-sync.service -f
```

## Tinh chỉnh tài nguyên (quan trọng nhất cho production)

| Biến | Ý nghĩa | Công thức trần connection |
|---|---|---|
| `WORKER_COUNT` | Số worker chạy song song | — |
| `PG_POOL_MAX` | Connection Postgres / worker | tổng ≈ `WORKER_COUNT × PG_POOL_MAX` |
| `MONGO_MAX_POOL_SIZE` | Connection MongoDB / worker | tổng ≈ `WORKER_COUNT × MONGO_MAX_POOL_SIZE` |

Trước khi tăng `WORKER_COUNT`, kiểm tra:
- `SHOW max_connections;` trên Postgres, trừ hao cho các app khác đang dùng.
- Giới hạn connection hiện tại của MongoDB (`db.serverStatus().connections`).
- Nếu tăng `WORKER_COUNT` hoặc `WORKER_MAX_OLD_GEN_MB`, tăng tương ứng
  `mem_limit`/`cpus` trong `docker-compose.yml` (công thức thô đã ghi chú
  ngay trong file).

Nếu vẫn thấy tải cao trên MongoDB/Postgres production dù đã giới hạn
connection, hạ tiếp `WORKER_COUNT` xuống 1-2, và/hoặc bật `BATCH_DELAY_MS`
(vd `500`) để giãn tốc độ ghi ra.

## Đừng bỏ qua: index cho `$lookup`

Giới hạn tài nguyên ở container **chỉ kiểm soát lượng tài nguyên script
được PHÉP dùng** — nó không sửa được nguyên nhân gốc nếu MongoDB phải quét
collection cho mỗi `$lookup` vì thiếu index. Script đã tự log cảnh báo này
khi treo quá `MONGO_IDLE_TIMEOUT_SECS`. Theo pipeline hiện tại, nên có index
phủ tối thiểu:

- Collection nguồn (`trans`, biến `MONGO_COLLECTION`): `(transtype, state, result)`.
- `agent_trans`: `(transid, dc, state, result)`.
- `agents`: `(reference)`.
- `bonus_in` / `bonus_out`: theo các trường dùng trong `$match` của từng
  `$lookup` con (`transType`, `scode`, `pcode`, `connector`, `fromDate`,
  `toDate`, `targetCode`...).

Thiếu các index này, dù container có giới hạn CPU/RAM chặt tới đâu, MongoDB
server vẫn phải làm việc nặng cho mỗi lần `$lookup` — đây thường là nguyên
nhân chính gây "ảnh hưởng cả server" ở phía MongoDB, độc lập với việc có
Docker hoá script hay không.

## Kiểm tra trước khi build thật

Sandbox tạo ra các file này **không có mạng ra ngoài**, nên chưa build/run
Docker image được ở đây. Trước khi dùng production:

```bash
# 1. kiem tra script khong loi cu phap (da lam trong sandbox)
node --check sync-mongo-to-pg.js

# 2. build image that
docker compose build

# 3. chay thu voi LIMIT_DOCS nho + tro toi 1 PG_TABLE test (khong phai bang that)
#    de xac nhan ket noi + quyen ghi truoc khi chay full
```

Chưa có `package-lock.json` đi kèm (môi trường tạo file không truy cập được
npm registry) — `Dockerfile` tự fallback sang `npm install` nếu không thấy
lockfile. Khuyến nghị: chạy `npm install` một lần ở máy có mạng để sinh
`package-lock.json`, commit nó vào repo để lần build sau dùng `npm ci`
(nhanh hơn, reproducible hơn).
