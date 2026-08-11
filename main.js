#!/usr/bin/env node
'use strict';

/**
 * sync-mongo-to-pg.js
 * -----------------------------------------------------------------------
 * Đồng bộ dữ liệu từ MongoDB (aggregation pipeline) sang PostgreSQL.
 * Bản chuyển từ sync_mongo_to_pg.sh sang Node.js thuần — dùng thẳng driver
 * `mongodb` và `pg` thay vì gọi ra mongosh/psql/jq qua shell.
 *
 * Cách hoạt động (bản BATCH-COMMIT, ổn định hơn bản 1-transaction cũ):
 *   1. Đảm bảo schema + bảng đích tồn tại trên Postgres
 *   2. Chạy aggregation pipeline (pipeline.json) trên MongoDB, đọc kết quả
 *      qua cursor (streaming, không load hết vào RAM)
 *   3. Gom từng nhóm BATCH_SIZE dòng (mặc định 10.000), COPY vào staging
 *      TEMP TABLE rồi UPSERT (INSERT ... ON CONFLICT) sang bảng đích và
 *      COMMIT ngay cho batch đó — KHÔNG đợi đọc hết toàn bộ Mongo rồi mới
 *      commit 1 lần như bản cũ.
 *   4. Lặp lại bước 3 cho tới khi cursor Mongo hết dữ liệu.
 *
 * Vì sao đổi sang batch-commit:
 *   - Bản cũ mở 1 transaction Postgres duy nhất bao trọn toàn bộ quá trình
 *     fetch (có thể chạy nhiều giờ với hàng triệu dòng). Nếu tiến trình bị
 *     treo/crash/OOM ở BẤT KỲ đâu — kể cả khi đã xử lý xong 99% — toàn bộ
 *     transaction bị rollback (hoặc treo giữ lock nếu bị kill -9), không có
 *     dòng nào được ghi thật vào bảng đích. Lần chạy lại phải làm lại từ đầu.
 *   - Bản batch-commit: mỗi batch COMMIT riêng, độc lập với các batch khác.
 *     Nếu sự cố xảy ra ở batch thứ N, N-1 batch trước đó đã nằm an toàn
 *     trong bảng đích rồi.
 *   - Vì UPSERT (ON CONFLICT DO UPDATE) là idempotent, nếu script dừng giữa
 *     chừng (lỗi, Ctrl+C, hay treo), CHỈ CẦN CHẠY LẠI SCRIPT TỪ ĐẦU — không
 *     cần dọn dẹp gì cả. Các dòng đã đồng bộ trước đó chỉ bị ghi đè bằng
 *     đúng giá trị cũ (rẻ), không tạo trùng, không lỗi.
 *
 * Cơ chế phát hiện & xử lý "treo" mới thêm:
 *   - MONGO_IDLE_TIMEOUT_SECS: nếu không nhận được dòng MỚI nào từ MongoDB
 *     trong X giây (mặc định 120s), coi như bị treo (thường do THIẾU INDEX
 *     ở $lookup) -> tự huỷ cursor và dừng lại, giữ nguyên các batch đã
 *     commit. Đặt 0 để tắt.
 *   - MONGO_MAX_TIME_SECS (tuỳ chọn): giới hạn thời gian ở phía SERVER
 *     MongoDB cho lệnh aggregate. Lưu ý: đây chủ yếu chặn bước khởi tạo
 *     cursor, KHÔNG chắc chắn áp dụng cho mọi getMore về sau tuỳ phiên bản
 *     server/driver — nên MONGO_IDLE_TIMEOUT_SECS ở client vẫn là tuyến
 *     phòng thủ chính cho việc treo giữa chừng.
 *   - PG_MAX_RETRIES / PG_RETRY_DELAY_SECS: nếu ghi 1 batch vào Postgres bị
 *     lỗi (vd mất kết nối tạm thời), tự động mở lại kết nối và thử lại
 *     batch đó (delay tăng dần, tối đa 60s/lần), thay vì chết luôn.
 *   - Ctrl+C (SIGINT/SIGTERM) lần đầu: dừng an toàn sau khi batch đang xử lý
 *     commit xong (không cắt ngang 1 transaction đang chạy dở). Lần thứ hai:
 *     thoát ngay lập tức.
 *
 * Cài đặt:
 *   npm install
 *
 * Cách chạy:
 *   MONGO_URI="mongodb://user:pass@host:27017/mydb" \
 *   PG_DB="mydb" PG_USER="pguser" PGPASSWORD="pgpass" PG_SCHEMA="mart" \
 *   BATCH_SIZE=10000 MONGO_IDLE_TIMEOUT_SECS=120 \
 *   node sync-mongo-to-pg.js
 *
 * Xem thêm các biến môi trường hỗ trợ trong file .env.example (nhớ bổ sung
 * các biến mới: BATCH_SIZE, MONGO_IDLE_TIMEOUT_SECS, MONGO_MAX_TIME_SECS,
 * PG_MAX_RETRIES, PG_RETRY_DELAY_SECS — có giá trị mặc định hợp lý nên
 * không bắt buộc phải set).
 * -----------------------------------------------------------------------
 */

const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');
const { pipeline: streamPipeline } = require('stream/promises');
const { MongoClient, ObjectId, Long, Decimal128 } = require('mongodb');
const { Pool } = require('pg');
const copyFrom = require('pg-copy-streams').from;
const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');

// ================== CẤU HÌNH (override bằng biến môi trường) ==================

function requiredEnv(name, hint) {
  const v = process.env[name];
  if (!v) {
    console.error(`Cần set biến môi trường ${name}${hint ? ` (${hint})` : ''}`);
    process.exit(1);
  }
  return v;
}

function positiveIntEnv(name, def) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
}

function nonNegativeIntEnv(name, def) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : def;
}

const MONGO_URI = requiredEnv('MONGO_URI', 'vd: mongodb://user:pass@host:27017/mydb');
// Tên collection gốc dùng ở $match đầu pipeline (KHÔNG PHẢI agent_trans/agents,
// đó là các collection được $lookup bên trong pipeline.json). Sửa nếu tên thực tế khác "trans".
const MONGO_COLLECTION = process.env.MONGO_COLLECTION || 'trans';

const PG_HOST = process.env.PG_HOST || 'localhost';
const PG_PORT = Number(process.env.PG_PORT || 5432);
const PG_DB = requiredEnv('PG_DB');
const PG_USER = requiredEnv('PG_USER');
const PG_PASSWORD = requiredEnv('PGPASSWORD');
const PG_SCHEMA = process.env.PG_SCHEMA || 'mart'; // schema chứa bảng đích
const PG_TABLE = process.env.PG_TABLE || 'agent_trans_sync'; // chỉ chữ/số/gạch dưới, không nhận input từ bên ngoài

if (!/^[a-zA-Z0-9_]+$/.test(PG_SCHEMA) || !/^[a-zA-Z0-9_]+$/.test(PG_TABLE)) {
  console.error('PG_SCHEMA / PG_TABLE chỉ được chứa chữ, số và gạch dưới.');
  process.exit(1);
}
const PG_TABLE_FQ = `${PG_SCHEMA}.${PG_TABLE}`;

const SCRIPT_DIR = __dirname;
const PIPELINE_FILE = process.env.PIPELINE_FILE || path.join(SCRIPT_DIR, 'pipeline.json');

// Chống treo vô thời hạn + hỗ trợ test nhanh với ít dữ liệu:
const MONGO_PING_TIMEOUT_MS = Number(process.env.MONGO_PING_TIMEOUT_SECS || 15) * 1000; // timeout riêng cho bước kiểm tra kết nối
const MONGO_TIMEOUT_MS = Number(process.env.MONGO_TIMEOUT_SECS || 0) * 1000; // giới hạn TỔNG thời gian fetch+ghi (0 = không giới hạn). Vì giờ commit theo batch, để 0 là an toàn với dữ liệu lớn.
const MONGO_IDLE_TIMEOUT_MS = nonNegativeIntEnv('MONGO_IDLE_TIMEOUT_SECS', 120) * 1000; // KHÔNG có dòng mới nào trong X giây -> coi là treo, tự dừng (0 = tắt). Đây là tuyến phòng thủ chính chống treo.
const MONGO_MAX_TIME_MS = nonNegativeIntEnv('MONGO_MAX_TIME_SECS', 0) * 1000; // tuỳ chọn: giới hạn phía server cho lệnh aggregate (0 = không giới hạn)
const LIMIT_DOCS = Number(process.env.LIMIT_DOCS || 0); // >0: thêm $limit vào pipeline để test nhanh, vd LIMIT_DOCS=20

const BATCH_SIZE = positiveIntEnv('BATCH_SIZE', 2000); // số dòng gom mỗi lần COPY + UPSERT + COMMIT
const PG_MAX_RETRIES = nonNegativeIntEnv('PG_MAX_RETRIES', 5); // số lần thử lại khi ghi 1 batch vào Postgres bị lỗi (vd mất kết nối)
const PG_RETRY_DELAY_SECS = nonNegativeIntEnv('PG_RETRY_DELAY_SECS', 5); // độ trễ cơ sở giữa các lần thử lại (tăng dần theo số lần thử, tối đa 60s)

// Log tiến độ khi xử lý dữ liệu lớn:
const PROGRESS_INTERVAL_MS = Number(process.env.PROGRESS_INTERVAL_SECS || 10) * 1000; // tần suất in tiến độ đọc từ Mongo
const TOTAL_DOCS_HINT = Number(process.env.TOTAL_DOCS_HINT || 0); // >0: tự cung cấp tổng số dòng ước tính để hiện thêm %
const IDLE_CHECK_INTERVAL_MS = 5000; // tần suất watchdog kiểm tra idle timeout

const WORKER_COUNT = isMainThread
  ? positiveIntEnv('WORKER_COUNT', 10)
  : Number(workerData.workerCount);
const WORKER_INDEX = isMainThread ? -1 : Number(workerData.workerIndex);

if (!isMainThread) {
  if (!Number.isInteger(WORKER_COUNT) || WORKER_COUNT <= 0) {
    throw new Error(`WORKER_COUNT không hợp lệ: ${WORKER_COUNT}`);
  }
  if (!Number.isInteger(WORKER_INDEX) || WORKER_INDEX < 0 || WORKER_INDEX >= WORKER_COUNT) {
    throw new Error(`WORKER_INDEX không hợp lệ: ${WORKER_INDEX}/${WORKER_COUNT}`);
  }
}

// Danh sách cột nạp vào Postgres (không gồm synced_at, cột này có DEFAULT now())
const COLUMNS = [
  'agent_trans_id', 'transid', 'amount', 'dc', 'date', 'transtype', 'result', 'state',
  'connector', 'agent_ref', 're_agent_ref', 'agentid', 're_agentid', 'reqid',
  'scode', 'pcode', 'tcode', 'agent_type', 'reference', 'debitor', 'creditor', 'last_update',
  'bonus_in_amount', 'bonus_out_amount', 'bonus_in_data', 'bonus_out_data', 'refund_transid', 'refund_amount', 'default_wallet_type'
];
// Các field dạng "id" cần bóc khỏi ObjectId/Long/Decimal128 nếu Mongo trả về kiểu đó
const ID_LIKE_FIELDS = new Set(['agent_trans_id', 'transid', 'agentid', 're_agentid', 'reqid']);

// ================== LOG ==================

function log(...args) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const scope = isMainThread ? '[master]' : `[worker ${WORKER_INDEX + 1}/${WORKER_COUNT}]`;
  console.error(`[${ts}] ${scope}`, ...args);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ================== TIỆN ÍCH CHUYỂN DỮ LIỆU MONGO -> TSV ==================

// Bóc giá trị khỏi các kiểu BSON đặc thù (ObjectId, Long, Decimal128) về dạng text
function unwrapIdLike(value) {
  if (value === null || value === undefined) return null;
  if (value instanceof ObjectId) return value.toHexString();
  if (Long && Long.isLong && Long.isLong(value)) return value.toString();
  if (Decimal128 && value instanceof Decimal128) return value.toString();
  return value;
}

// Field 'date' trong Mongo có thể là BSON Date thật, hoặc chỉ là số epoch-ms
// thô (Int32/Int64/Double) tuỳ cách dữ liệu được ghi vào. Chuẩn hoá cả 2
// trường hợp về chuỗi ISO 8601 để COPY vào cột timestamptz không bị lỗi
// "date/time field value out of range" khi Postgres cố hiểu số thô như text.
function toTimestampIso(value) {
  if (value === null || value === undefined) return null;
  let d;
  if (value instanceof Date) {
    d = value;
  } else if (typeof value === 'number') {
    d = new Date(value); // epoch milliseconds
  } else if (Long && Long.isLong && Long.isLong(value)) {
    d = new Date(value.toNumber()); // epoch milliseconds
  } else {
    d = new Date(value); // thử parse chuỗi ISO có sẵn hoặc kiểu khác
  }
  return isNaN(d.getTime()) ? null : d.toISOString();
}

// Escape cho định dạng TEXT mặc định của Postgres COPY (tab-delimited, \N = null)
function escTsv(value) {
  if (value === null || value === undefined) return '\\N';
  let str;
  if (value instanceof Date) {
    str = value.toISOString();
  } else {
    str = String(value);
  }
  return str
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, '\\t')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}

function normalizeInteger(value) {
  if (value == null || value === '') return null;

  const numberValue = Number(value);
  if (!Number.isFinite(numberValue)) return null;

  return Math.round(numberValue);
}

function docToTsvLine(doc) {
  const values = COLUMNS.map((col) => {
    const raw = doc[col] === undefined ? null : doc[col];
    let value;

    if (['date', 'last_update'].includes(col)) {
      value = toTimestampIso(raw);
    } else if (ID_LIKE_FIELDS.has(col)) {
      value = unwrapIdLike(raw);
    } else if (['bonus_in_data', 'bonus_out_data'].includes(col)) {
      value = raw == null ? '' : JSON.stringify(raw);
    } else if (['bonus_in_amount', 'bonus_out_amount', 'agent_type', 'default_wallet_type'].includes(col)) {
      value = normalizeInteger(raw);
    } else {
      value = raw;
    }

    // log(
    //   `column=${col} raw=${String(raw)}`
    // );

    return escTsv(value);
  });

  return values.join('\t') + '\n';
}

// ================== BƯỚC 1: ĐẢM BẢO SCHEMA + BẢNG ĐÍCH TỒN TẠI ==================

async function ensurePostgresSchema(pool) {
  log("[Bước 1/2] Đảm bảo schema '" + PG_SCHEMA + "' và bảng '" + PG_TABLE_FQ + "' tồn tại...");
  const ddl = `
    CREATE SCHEMA IF NOT EXISTS ${PG_SCHEMA};

    CREATE TABLE IF NOT EXISTS ${PG_TABLE_FQ} (
        agent_trans_id        text PRIMARY KEY,
        transid               bigint,
        amount                integer,
        dc                    text,
        date                  timestamptz,
        transtype             text,
        result                integer,
        state                 integer,
        connector             text,
        agent_ref             text,
        re_agent_ref          text,
        agentid               text,
        re_agentid            text,
        reqid                 text,
        scode                 text,
        pcode                 text,
        tcode                 text,
        agent_type            integer,
        default_wallet_type   integer,
        reference             text,
        debitor               text,
        creditor              text,
        bonus_in_amount       integer,
        bonus_out_amount      integer,
        bonus_in_data         text,
        bonus_out_data        text,
        refund_transid        integer,
        refund_amount         integer,
        last_update           timestamptz,
        synced_at             timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_${PG_TABLE}_transid ON ${PG_TABLE_FQ} (transid);
    CREATE INDEX IF NOT EXISTS idx_${PG_TABLE}_date    ON ${PG_TABLE_FQ} (date);
    CREATE INDEX IF NOT EXISTS idx_${PG_TABLE}_last_update    ON ${PG_TABLE_FQ} (last_update);
  `;
  await pool.query(ddl);
  log(`Sẵn sàng. Bảng đích: '${PG_TABLE_FQ}'.`);
}

// Tạo staging TEMP TABLE cho 1 session Postgres. KHÔNG dùng "ON COMMIT DROP":
// vì giờ 1 session có thể COMMIT nhiều lần (mỗi batch 1 lần), ON COMMIT DROP
// sẽ xoá bảng ngay sau batch đầu tiên. Bảng temp sẽ tự bị dọn khi session
// (connection) đóng, nên không cần drop thủ công.
async function ensureStagingTable(pgClient) {
  await pgClient.query(
    `CREATE TEMP TABLE IF NOT EXISTS staging_sync (LIKE ${PG_TABLE_FQ} INCLUDING DEFAULTS)`
  );
}

// ================== BƯỚC 2: KIỂM TRA KẾT NỐI MONGO + NẠP PIPELINE ==================

async function pingMongo(client) {
  log(`Kiểm tra kết nối MongoDB (timeout ${MONGO_PING_TIMEOUT_MS / 1000}s)...`);
  const pingPromise = client.db().admin().command({ ping: 1 });
  const timeoutPromise = new Promise((_, reject) =>
    setTimeout(
      () => reject(new Error(`Không kết nối được MongoDB trong ${MONGO_PING_TIMEOUT_MS / 1000}s`)),
      MONGO_PING_TIMEOUT_MS
    )
  );
  try {
    await Promise.race([pingPromise, timeoutPromise]);
  } catch (err) {
    log('Kiểm tra: VPN/mạng tới host trong MONGO_URI, user/pass, tên replicaSet.');
    throw err;
  }
  log('Kết nối MongoDB OK.');
}

function loadPipeline() {
  // if (!fs.existsSync(PIPELINE_FILE)) {
  //   throw new Error(`Không tìm thấy pipeline file: ${PIPELINE_FILE}`);
  // }
  // const pipeline = JSON.parse(fs.readFileSync(PIPELINE_FILE, 'utf8'));
  // if (LIMIT_DOCS > 0) {
  //   pipeline.push({ $limit: LIMIT_DOCS });
  //   log(`Chế độ test nhanh: giới hạn LIMIT_DOCS=${LIMIT_DOCS} dòng.`);
  // }
  // return pipeline;
  const pipeline = [
    {
      "$match": {
        "transtype": {
          "$in": [
            "BUY",
            "BUYCODE",
            "BILLPAY"
          ]
        },
        "state": 2,
        "result": 0
      }
    },
    {
      "$group": {
        "_id": "$transid",
        "doc": {
          "$first": "$$ROOT"
        }
      }
    },
    {
      "$replaceRoot": {
        "newRoot": "$doc"
      }
    },
    {
      "$lookup": {
        "from": "agent_trans",
        "let": {
          "lid": "$transid"
        },
        "pipeline": [
          {
            "$match": {
              "$expr": {
                "$and": [
                  {
                    "$eq": [
                      "$transid",
                      "$$lid"
                    ]
                  },
                  {
                    "$in": [
                      "$dc",
                      [
                        "D",
                        "C"
                      ]
                    ]
                  },
                  {
                    "$eq": [
                      "$state",
                      2
                    ]
                  },
                  {
                    "$eq": [
                      "$result",
                      0
                    ]
                  }
                ]
              }
            }
          },
          {
            "$project": {
              "_id": 1,
              "transid": 1,
              "amount": 1,
              "date": 1,
              "dc": 1,
              "transtype": 1,
              "result": 1,
              "state": 1,
              "connector": 1,
              "agent_ref": 1,
              "re_agent_ref": 1,
              "agentid": 1,
              "re_agentid": 1,
              "reqid": 1
            }
          }
        ],
        "as": "agent_trans_rows"
      }
    },
    {
      "$addFields": {
        "agent_trans_d": {
          "$arrayElemAt": [
            {
              "$filter": {
                "input": "$agent_trans_rows",
                "as": "item",
                "cond": {
                  "$eq": [
                    "$$item.dc",
                    "D"
                  ]
                }
              }
            },
            0
          ]
        }
      }
    },
    {
      "$unwind": {
        "path": "$agent_trans_rows",
        "preserveNullAndEmptyArrays": false
      }
    },
    {
      "$addFields": {
        "current_agent_trans": "$agent_trans_rows"
      }
    },
    {
      "$addFields": {
        "cardObj": {
          "$cond": {
            "if": {
              "$isArray": "$card"
            },
            "then": {
              "$arrayElemAt": [
                "$card",
                0
              ]
            },
            "else": "$card"
          }
        },
        "dataObj": {
          "$cond": {
            "if": {
              "$isArray": "$data"
            },
            "then": {
              "$arrayElemAt": [
                "$data",
                0
              ]
            },
            "else": "$data"
          }
        },
        "specialAgentFirst": {
          "$arrayElemAt": [
            {
              "$split": [
                {
                  "$ifNull": [
                    "$special_agent",
                    ""
                  ]
                },
                "."
              ]
            },
            0
          ]
        }
      }
    },
    {
      "$addFields": {
        "cardType": {
          "$ifNull": [
            "$cardObj.cardtype",
            "$cardObj.cardType"
          ]
        },
        "cardPcode": {
          "$ifNull": [
            "$cardObj.providercode",
            "$cardObj.providerCode"
          ]
        }
      }
    },
    {
      "$addFields": {
        "filled_date": {
          "$ifNull": [
            "$current_agent_trans.date",
            "$agent_trans_d.date"
          ]
        },
        "filled_transtype": {
          "$ifNull": [
            "$current_agent_trans.transtype",
            "$agent_trans_d.transtype"
          ]
        },
        "filled_result": {
          "$ifNull": [
            "$current_agent_trans.result",
            "$agent_trans_d.result"
          ]
        },
        "filled_state": {
          "$ifNull": [
            "$current_agent_trans.state",
            "$agent_trans_d.state"
          ]
        },
        "filled_connector": {
          "$ifNull": [
            "$current_agent_trans.connector",
            "$agent_trans_d.connector"
          ]
        },
        "filled_agent_ref": {
          "$ifNull": [
            "$current_agent_trans.agent_ref",
            "$agent_trans_d.agent_ref"
          ]
        },
        "filled_re_agent_ref": {
          "$ifNull": [
            "$current_agent_trans.re_agent_ref",
            "$agent_trans_d.re_agent_ref"
          ]
        },
        "filled_agentid": {
          "$ifNull": [
            "$current_agent_trans.agentid",
            "$agent_trans_d.agentid"
          ]
        },
        "filled_re_agentid": {
          "$ifNull": [
            "$current_agent_trans.re_agentid",
            "$agent_trans_d.re_agentid"
          ]
        },
        "filled_reqid": {
          "$ifNull": [
            "$current_agent_trans.reqid",
            "$agent_trans_d.reqid"
          ]
        }
      }
    },
    {
      "$lookup": {
        "from": "agents",
        "let": {
          "aref": {
            "$switch": {
              "branches": [
                {
                  "case": {
                    "$eq": ["$current_agent_trans.dc", "D"]
                  },
                  "then": "$current_agent_trans.agent_ref"
                },
                {
                  "case": {
                    "$eq": ["$current_agent_trans.dc", "C"]
                  },
                  "then": "$current_agent_trans.re_agent_ref"
                }
              ],
              "default": null
            }
          }
        },
        "pipeline": [
          {
            "$match": {
              "$expr": {
                "$eq": [
                  "$$aref",
                  "$reference"
                ]
              }
            }
          },
          {
            "$project": {
              "reference": 1,
              "agent_type": 1,
              "default_wallet_type": 1
            }
          }
        ],
        "as": "agents"
      }
    },
    {
      "$unwind": {
        "path": "$agents",
        "preserveNullAndEmptyArrays": true
      }
    },
    {
      "$project": {
        "agent_trans_id": "$current_agent_trans._id",
        "transid": 1,
        "amount": "$current_agent_trans.amount",
        "dc": "$current_agent_trans.dc",
        "date": "$filled_date",
        "transtype": "$filled_transtype",
        "result": "$filled_result",
        "state": "$filled_state",
        "connector": "$filled_connector",
        "agent_ref": "$filled_agent_ref",
        "re_agent_ref": "$filled_re_agent_ref",
        "agentid": "$filled_agentid",
        "re_agentid": "$filled_re_agentid",
        "reqid": "$filled_reqid",
        "scode": {
          "$switch": {
            "branches": [
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BUY"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$scode",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "topup"
              },
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BILLPAY"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$scode",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "$dataObj.scode"
              }
            ],
            "default": "$scode"
          }
        },
        "pcode": {
          "$switch": {
            "branches": [
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BUY"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$pcode",
                            null
                          ]
                        },
                        null
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$telco",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "$specialAgentFirst"
              },
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BUY"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$pcode",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "$telco"
              },
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BUYCODE"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$pcode",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "$cardPcode"
              },
              {
                "case": {
                  "$eq": [
                    {
                      "$ifNull": [
                        "$pcode",
                        null
                      ]
                    },
                    null
                  ]
                },
                "then": "$dataObj.pcode"
              }
            ],
            "default": "$pcode"
          }
        },
        "tcode": {
          "$switch": {
            "branches": [
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BUYCODE"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$tcode",
                            null
                          ]
                        },
                        null
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$cardType",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "$dataObj.amount"
              },
              {
                "case": {
                  "$and": [
                    {
                      "$eq": [
                        "$transtype",
                        "BUYCODE"
                      ]
                    },
                    {
                      "$eq": [
                        {
                          "$ifNull": [
                            "$tcode",
                            null
                          ]
                        },
                        null
                      ]
                    }
                  ]
                },
                "then": "$cardType"
              }
            ],
            "default": "$tcode"
          }
        },
        "agent_type": "$agents.agent_type",
        "default_wallet_type": "$agents.default_wallet_type",
        "reference": "$agents.reference",
        "debitor": "$debitor",
        "creditor": "$creditor",
        "last_update": "$lastupdate"
      }
    },
    {
      "$lookup": {
        "from": "bonus_in",
        "let": {
          "ltransType": "$transtype",
          "lscode": "$scode",
          "lpcode": "$pcode",
          "lconnector": "$connector",
          "ltcode": "$tcode",
          "ldate": "$date",
          "ldc": "$dc"
        },
        "pipeline": [
          {
            "$addFields": {
              "fromTs": {
                "$toLong": "$fromDate"
              },
              "toTs": {
                "$cond": [
                  {
                    "$ne": [
                      "$toDate",
                      null
                    ]
                  },
                  {
                    "$toLong": "$toDate"
                  },
                  null
                ]
              }
            }
          },
          {
            "$match": {
              "$expr": {
                "$and": [
                  {
                    "$eq": [
                      "$$ldc",
                      "D"
                    ]
                  },
                  {
                    "$eq": [
                      "$status",
                      "active"
                    ]
                  },
                  {
                    "$eq": [
                      "$transType",
                      "$$ltransType"
                    ]
                  },
                  {
                    "$eq": [
                      "$$lscode",
                      "$scode"
                    ]
                  },
                  {
                    "$eq": [
                      "$$lpcode",
                      "$pcode"
                    ]
                  },
                  {
                    "$eq": [
                      "$connector",
                      "$$lconnector"
                    ]
                  },
                  {
                    "$lte": [
                      "$fromTs",
                      "$$ldate"
                    ]
                  },
                  {
                    "$or": [
                      {
                        "$eq": [
                          "$toDate",
                          null
                        ]
                      },
                      {
                        "$gte": [
                          "$toTs",
                          "$$ldate"
                        ]
                      }
                    ]
                  },
                  {
                    "$or": [
                      {
                        "$and": [
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      {
                        "$eq": [
                          "$targetCode",
                          null
                        ]
                      },
                    ]
                  }
                ]
              }
            }
          },
          {
            "$addFields": {
              "matchLevel": {
                "$switch": {
                  "branches": [
                    {
                      "case": {
                        "$and": [
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      "then": 1
                    },
                    {
                      "case": {
                        "$eq": [
                          "$targetCode",
                          null
                        ]
                      },
                      "then": 2
                    }
                  ],
                  "default": 99
                }
              }
            }
          },
          {
            "$sort": {
              "matchLevel": 1,
              "priority": -1,
              "fromDate": -1
            }
          },
          {
            "$limit": 1
          }
        ],
        "as": "bonus_in"
      }
    },
    {
      "$lookup": {
        "from": "bonus_out",
        "let": {
          "ltransType": "$transtype",
          "lscode": "$scode",
          "lpcode": "$pcode",
          "lagentType": "$agent_type",
          "lreference": "$reference",
          "ltcode": "$tcode",
          "ldate": "$date",
          "ldc": "$dc"
        },
        "pipeline": [
          {
            "$addFields": {
              "fromTs": {
                "$toLong": "$fromDate"
              },
              "toTs": {
                "$cond": [
                  {
                    "$ne": [
                      "$toDate",
                      null
                    ]
                  },
                  {
                    "$toLong": "$toDate"
                  },
                  null
                ]
              },
              "safeReferences": {
                "$cond": [
                  {
                    "$isArray": "$references"
                  },
                  "$references",
                  []
                ]
              }
            }
          },
          {
            "$match": {
              "$expr": {
                "$and": [
                  {
                    "$eq": [
                      "$$ldc",
                      "D"
                    ]
                  },
                  {
                    "$eq": [
                      "$status",
                      "active"
                    ]
                  },
                  {
                    "$eq": [
                      "$transType",
                      "$$ltransType"
                    ]
                  },
                  {
                    "$eq": [
                      "$$lscode",
                      "$scode"
                    ]
                  },
                  {
                    "$eq": [
                      "$$lpcode",
                      "$pcode"
                    ]
                  },
                  {
                    "$lte": [
                      "$fromTs",
                      "$$ldate"
                    ]
                  },
                  {
                    "$or": [
                      {
                        "$eq": [
                          "$toDate",
                          null
                        ]
                      },
                      {
                        "$gte": [
                          "$toTs",
                          "$$ldate"
                        ]
                      }
                    ]
                  },
                  {
                    "$or": [
                      // agentType + reference + targetCode
                      {
                        "$and": [
                          {
                            "$eq": [
                              "$$lagentType",
                              "$agentType"
                            ]
                          },
                          {
                            "$gt": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$in": [
                              "$$lreference",
                              "$safeReferences"
                            ]
                          },
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      // agentType + targetCode
                      {
                        "$and": [
                          {
                            "$eq": [
                              "$$lagentType",
                              "$agentType"
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      // agentType + reference
                      {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              "$$lagentType"
                            ]
                          },
                          {
                            "$gt": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$in": [
                              "$$lreference",
                              "$safeReferences"
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              null
                            ]
                          }
                        ]
                      },
                      // agentType only
                      {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              "$$lagentType"
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              null
                            ]
                          }
                        ]
                      },
                      // targetCode only
                      {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      // agent type null + reference [] + targetCode null
                      {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              null
                            ]
                          }
                        ]
                      }
                    ]
                  }
                ]
              }
            }
          },
          {
            "$addFields": {
              "matchLevel": {
                "$switch": {
                  "branches": [
                    {
                      "case": {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              "$$lagentType"
                            ]
                          },
                          {
                            "$gt": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$in": [
                              "$$lreference",
                              "$safeReferences"
                            ]
                          },
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      "then": 1
                    },
                    {
                      "case": {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              "$$lagentType"
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      "then": 2
                    },
                    {
                      "case": {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              "$$lagentType"
                            ]
                          },
                          {
                            "$gt": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$in": [
                              "$$lreference",
                              "$safeReferences"
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              null
                            ]
                          }
                        ]
                      },
                      "then": 3
                    },
                    {
                      "case": {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              "$$lagentType"
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              null
                            ]
                          }
                        ]
                      },
                      "then": 4
                    },
                    {
                      "case": {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$ne": [
                              "$targetCode",
                              null
                            ]
                          },
                          {
                            "$ne": [
                              "$$ltcode",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              "$$ltcode"
                            ]
                          }
                        ]
                      },
                      "then": 5
                    },
                    {
                      "case": {
                        "$and": [
                          {
                            "$eq": [
                              "$agentType",
                              null
                            ]
                          },
                          {
                            "$eq": [
                              {
                                "$size": "$safeReferences"
                              },
                              0
                            ]
                          },
                          {
                            "$eq": [
                              "$targetCode",
                              null
                            ]
                          }
                        ]
                      },
                      "then": 6
                    }
                  ],
                  "default": 99
                }
              }
            }
          },
          {
            "$sort": {
              "matchLevel": 1,
              "priority": -1,
              "fromDate": -1
            }
          },
          {
            "$limit": 1
          }
        ],
        "as": "bonus_out"
      }
    },
    {
      "$project": {
        "agent_trans_id": 1,
        "transid": 1,
        "amount": 1,
        "dc": 1,
        "date": 1,
        "transtype": 1,
        "result": 1,
        "state": 1,
        "connector": 1,
        "agent_ref": 1,
        "re_agent_ref": 1,
        "agentid": 1,
        "re_agentid": 1,
        "reqid": 1,
        "scode": 1,
        "pcode": 1,
        "tcode": 1,
        "agent_type": 1,
        "default_wallet_type": 1,
        "reference": 1,
        "debitor": 1,
        "creditor": 1,
        "refund_transid": null,
        "refund_amount": null,
        "last_update": 1,
        "synced_at": 1,
        "bonus_in_amount": {
          "$let": {
            "vars": {
              "rule": {
                "$first": "$bonus_in"
              }
            },
            "in": {
              "$cond": [
                {
                  "$eq": [
                    "$$rule",
                    null
                  ]
                },
                0,
                {
                  "$let": {
                    "vars": {
                      "bonus": {
                        "$add": [
                          {
                            "$multiply": [
                              { $abs: "$amount" },
                              {
                                "$divide": [
                                  {
                                    "$ifNull": [
                                      "$$rule.percent",
                                      0
                                    ]
                                  },
                                  100
                                ]
                              }
                            ]
                          },
                          {
                            "$ifNull": [
                              "$$rule.baseAmount",
                              0
                            ]
                          }
                        ]
                      }
                    },
                    "in": {
                      "$let": {
                        "vars": {
                          "floorApplied": {
                            "$cond": [
                              {
                                "$ne": [
                                  "$$rule.minAmount",
                                  null
                                ]
                              },
                              {
                                "$max": [
                                  "$$bonus",
                                  "$$rule.minAmount"
                                ]
                              },
                              "$$bonus"
                            ]
                          }
                        },
                        "in": {
                          "$cond": [
                            {
                              "$ne": [
                                "$$rule.maxAmount",
                                null
                              ]
                            },
                            {
                              "$min": [
                                "$$floorApplied",
                                "$$rule.maxAmount"
                              ]
                            },
                            "$$floorApplied"
                          ]
                        }
                      }
                    }
                  }
                }
              ]
            }
          }
        },
        "bonus_out_amount": {
          "$let": {
            "vars": {
              "rule": {
                "$first": "$bonus_out"
              }
            },
            "in": {
              "$cond": [
                {
                  "$eq": [
                    "$$rule",
                    null
                  ]
                },
                0,
                {
                  "$let": {
                    "vars": {
                      "bonus": {
                        "$add": [
                          {
                            "$multiply": [
                              { $abs: "$amount" },
                              {
                                "$divide": [
                                  {
                                    "$ifNull": [
                                      "$$rule.percent",
                                      0
                                    ]
                                  },
                                  100
                                ]
                              }
                            ]
                          },
                          {
                            "$ifNull": [
                              "$$rule.baseAmount",
                              0
                            ]
                          }
                        ]
                      }
                    },
                    "in": {
                      "$let": {
                        "vars": {
                          "floorApplied": {
                            "$cond": [
                              {
                                "$ne": [
                                  "$$rule.minAmount",
                                  null
                                ]
                              },
                              {
                                "$max": [
                                  "$$bonus",
                                  "$$rule.minAmount"
                                ]
                              },
                              "$$bonus"
                            ]
                          }
                        },
                        "in": {
                          "$cond": [
                            {
                              "$ne": [
                                "$$rule.maxAmount",
                                null
                              ]
                            },
                            {
                              "$min": [
                                "$$floorApplied",
                                "$$rule.maxAmount"
                              ]
                            },
                            "$$floorApplied"
                          ]
                        }
                      }
                    }
                  }
                }
              ]
            }
          }
        },
        "bonus_in_data": {
          "$first": "$bonus_in"
        },
        "bonus_out_data": {
          "$first": "$bonus_out"
        }
      }
    }
  ];

  if (!isMainThread && WORKER_COUNT > 1) {
    pipeline.splice(1, 0, {
      $match: {
        $expr: {
          $eq: [
            {
              $mod: [
                {
                  $abs: {
                    $convert: {
                      input: '$transid',
                      to: 'long',
                      onError: 0,
                      onNull: 0,
                    },
                  },
                },
                WORKER_COUNT,
              ],
            },
            WORKER_INDEX,
          ],
        },
      },
    });
  }

  if (LIMIT_DOCS > 0) {
    pipeline.push({ $limit: LIMIT_DOCS });
    log(`Chế độ test nhanh: giới hạn LIMIT_DOCS=${LIMIT_DOCS} dòng cho worker này.`);
  }

  return pipeline;
}

// ================== ĐỌC DỮ LIỆU TỪ CURSOR (có theo dõi tiến độ + idle) ==================

function makeFetchContext() {
  const now = Date.now();
  return {
    count: 0,
    lastLog: now,
    start: now,
    lastDocTime: now, // thời điểm nhận dòng gần nhất, watchdog idle dùng cái này
    idleTriggered: false,
  };
}

// function maybeLogFetchProgress(ctx) {
//   const now = Date.now();
//   if (now - ctx.lastLog < PROGRESS_INTERVAL_MS) return;
//   ctx.lastLog = now;
//   const elapsedSec = (now - ctx.start) / 1000;
//   const rate = elapsedSec > 0 ? (ctx.count / elapsedSec).toFixed(0) : ctx.count;
//   let line = `[fetch] ${ctx.count} dòng đã đọc từ MongoDB - ${elapsedSec.toFixed(0)}s - ~${rate} dòng/s`;
//   if (TOTAL_DOCS_HINT > 0) {
//     const pct = ((ctx.count / TOTAL_DOCS_HINT) * 100).toFixed(1);
//     line += ` - ~${pct}% (ước tính theo TOTAL_DOCS_HINT)`;
//   }
//   log(line);
// }
function maybeLogFetchProgress(ctx) {
  const now = Date.now();
  if (now - ctx.lastLog < PROGRESS_INTERVAL_MS) return;
  ctx.lastLog = now;
  const elapsedSec = (now - ctx.start) / 1000;
  const rate = elapsedSec > 0 ? (ctx.count / elapsedSec).toFixed(0) : ctx.count;
  log(`[fetch] ${ctx.count} dòng đã đọc từ MongoDB (partition này) - ${elapsedSec.toFixed(0)}s - ~${rate} dòng/s`);

  if (!isMainThread && parentPort) {
    parentPort.postMessage({ type: 'progress', count: ctx.count });
  }
}

// Đọc 1 document từ cursor. Nếu cursor bị đóng bởi watchdog (do idle timeout)
// trong lúc đang chờ, next() sẽ reject -> báo lỗi rõ ràng thay vì lỗi driver
// khó hiểu.
async function readNextDoc(cursor, ctx) {
  let doc;
  try {
    doc = await cursor.next();
  } catch (err) {
    if (ctx.idleTriggered) {
      throw new Error(
        `Không nhận được dữ liệu mới từ MongoDB trong ${MONGO_IDLE_TIMEOUT_MS / 1000}s ` +
        `— nghi ngờ TREO do THIẾU INDEX ở $lookup, hoặc mất kết nối tới MongoDB.`
      );
    }
    throw err;
  }
  ctx.lastDocTime = Date.now();
  if (doc === null) return null; // cursor đã hết dữ liệu
  ctx.count++;
  maybeLogFetchProgress(ctx);
  return doc;
}

// Gom tối đa batchSize document thành 1 batch. Dừng sớm (batch có thể rỗng
// hoặc chưa đầy) nếu cursor hết dữ liệu hoặc có yêu cầu dừng (stopFlag).
async function fetchBatch(cursor, ctx, batchSize, stopFlag) {
  const batch = [];
  while (batch.length < batchSize) {
    if (stopFlag.value) break;
    const doc = await readNextDoc(cursor, ctx);
    if (doc === null) return { batch, cursorDone: true };
    batch.push(doc);
  }
  return { batch, cursorDone: false };
}

// ================== GHI 1 BATCH VÀO POSTGRES (staging + upsert + commit) ==================

// Đếm số dòng trong batch có agent_trans_id trùng với dòng khác TRONG CÙNG
// batch. Postgres không cho phép 1 câu INSERT ... ON CONFLICT DO UPDATE
// động vào cùng 1 dòng đích 2 lần, nên cần biết để cảnh báo (dữ liệu nguồn
// có nhiều doc 'trans' khác nhau cùng transid, khớp cùng 1 dòng agent_trans).
function countDuplicateKeysInBatch(batch) {
  const seen = new Set();
  let dupCount = 0;
  for (const doc of batch) {
    const raw = doc.agent_trans_id;
    if (raw === undefined || raw === null) continue;
    const key = String(unwrapIdLike(raw));
    if (seen.has(key)) dupCount++;
    else seen.add(key);
  }
  return dupCount;
}

async function commitBatch(pgClient, batch, batchNum) {
  const dupCount = countDuplicateKeysInBatch(batch);
  if (dupCount > 0) {
    log(
      `[batch ${batchNum}] Lưu ý: ${dupCount} dòng có agent_trans_id trùng với dòng khác trong cùng batch ` +
      `(nhiều doc 'trans' nguồn cùng transid, khớp cùng 1 dòng agent_trans) — sẽ dedup, chỉ giữ 1 bản ghi/agent_trans_id ` +
      `(dòng nào "thắng" là ngẫu nhiên có kiểm soát qua ctid; scode/pcode/tcode của bản bị loại sẽ không được ghi).`
    );
  }

  await pgClient.query('BEGIN');
  try {
    await pgClient.query('TRUNCATE staging_sync');

    const copyStream = pgClient.query(copyFrom(`COPY staging_sync (${COLUMNS.join(', ')}) FROM STDIN`));
    const tsvData = batch.map(docToTsvLine).join('');
    await streamPipeline(Readable.from([tsvData]), copyStream);

    const updateSet = COLUMNS.filter((c) => c !== 'agent_trans_id')
      .map((c) => `${c} = EXCLUDED.${c}`)
      .join(',\n        ');

    // DISTINCT ON (agent_trans_id) loại trùng NGAY TRONG batch trước khi upsert,
    // để không bao giờ dính lỗi "ON CONFLICT DO UPDATE command cannot affect
    // row a second time". ctid làm tiebreaker vì các cột nghiệp vụ khác giữa
    // các dòng trùng thường giống hệt nhau (cùng agent_trans nguồn).
    await pgClient.query(`
      INSERT INTO ${PG_TABLE_FQ} AS t (${COLUMNS.join(', ')}, synced_at)
      SELECT ${COLUMNS.join(', ')}, now()
      FROM (
        SELECT DISTINCT ON (agent_trans_id) ${COLUMNS.join(', ')}
        FROM staging_sync
        ORDER BY agent_trans_id, ctid DESC
      ) dedup
      ON CONFLICT (agent_trans_id) DO UPDATE SET
        ${updateSet},
        synced_at = now()
    `);

    await pgClient.query('COMMIT');
  } catch (err) {
    await pgClient.query('ROLLBACK').catch(() => { });
    throw err;
  }
}

// Mở kết nối Postgres mới (dùng khi kết nối cũ bị lỗi/mất), huỷ kết nối cũ,
// và tạo lại staging table (temp table gắn với session nên bị mất khi đổi
// connection).
async function getFreshPgClient(pgPool, prevClient, batchNum) {
  if (prevClient) {
    try {
      prevClient.release(new Error('replaced after batch error'));
    } catch (_) {
      /* ignore */
    }
  }
  log(`[batch ${batchNum}] Mở lại kết nối PostgreSQL mới...`);
  const client = await pgPool.connect();
  await ensureStagingTable(client);
  return client;
}

// err.code là mã SQLSTATE do server Postgres trả về (lỗi cú pháp, ràng buộc,
// kiểu dữ liệu...) — thử lại chắc chắn KHÔNG giúp ích vì batch sẽ luôn sinh
// ra cùng 1 lỗi. Chỉ 1 số ít mã (deadlock, serialization, quá tải kết nối)
// là đáng thử lại. Lỗi KHÔNG có err.code (ECONNRESET, socket hang up, "Connection
// terminated"...) thường là lỗi mạng/driver -> coi là tạm thời, cho thử lại.
const RETRYABLE_PG_CODES = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '53300', // too_many_connections
  '57P03', // cannot_connect_now
  '08000', '08003', '08001', '08004', '08006', // connection_exception (nhóm)
]);

function isRetryablePgError(err) {
  const code = err && err.code;
  if (!code) return true;
  return RETRYABLE_PG_CODES.has(code);
}

async function commitBatchWithRetry(pgPool, clientRef, batch, batchNum) {
  let attempt = 0;
  while (true) {
    attempt++;
    try {
      await commitBatch(clientRef.current, batch, batchNum);
      return;
    } catch (err) {
      if (!isRetryablePgError(err)) {
        throw new Error(
          `[batch ${batchNum}] Ghi PostgreSQL thất bại do lỗi dữ liệu/logic (không phải mất kết nối, ` +
          `thử lại sẽ không giúp ích): ${err.message}`
        );
      }
      if (attempt > PG_MAX_RETRIES) {
        throw new Error(
          `[batch ${batchNum}] Ghi PostgreSQL thất bại sau ${attempt} lần thử: ${err.message}`
        );
      }
      const delayMs = Math.min(PG_RETRY_DELAY_SECS * 1000 * attempt, 60000);
      log(
        `[batch ${batchNum}] Lỗi khi ghi PostgreSQL (lần ${attempt}/${PG_MAX_RETRIES}): ${err.message}. ` +
        `Thử lại sau ${(delayMs / 1000).toFixed(0)}s...`
      );
      await sleep(delayMs);
      try {
        clientRef.current = await getFreshPgClient(pgPool, clientRef.current, batchNum);
      } catch (reErr) {
        log(`[batch ${batchNum}] Chưa mở lại được kết nối PostgreSQL: ${reErr.message}`);
      }
    }
  }
}

// ================== DỪNG AN TOÀN KHI NHẬN CTRL+C / SIGTERM ==================

function setupSignalHandlers(onStop) {
  let triggered = false;
  const handler = (sig) => {
    if (triggered) {
      log(`Nhận thêm tín hiệu ${sig} — thoát ngay lập tức (có thể bỏ dở batch đang xử lý).`);
      process.exit(130);
    }
    triggered = true;
    log(
      `Nhận tín hiệu ${sig} — sẽ dừng an toàn sau khi batch hiện tại commit xong ` +
      `(nhấn lần nữa để thoát ngay lập tức).`
    );
    onStop(sig);
  };
  process.on('SIGINT', handler);
  process.on('SIGTERM', handler);
  return () => {
    process.off('SIGINT', handler);
    process.off('SIGTERM', handler);
  };
}

// ================== VÒNG LẶP CHÍNH: FETCH THEO BATCH + COMMIT TỪNG BATCH ==================

async function runAggregationAndLoad(mongoClient, pgPool, checkExternalStop = () => false) {
  log(
    `[Bước 2/2] Bắt đầu đồng bộ partition ${WORKER_INDEX}/${WORKER_COUNT} theo batch ` +
    `(BATCH_SIZE=${BATCH_SIZE}, MONGO_IDLE_TIMEOUT_SECS=${MONGO_IDLE_TIMEOUT_MS / 1000}, ` +
    `collection nguồn: ${MONGO_COLLECTION})...`
  );
  log('Lưu ý: nếu tốc độ đọc từ MongoDB rất chậm hoặc treo, khả năng cao là THIẾU INDEX.');
  log('Kiểm tra index phủ (transid, dc, state, result) trên collection nguồn,');
  log('và index phủ (reference) trên collection được $lookup.');

  const pipeline = loadPipeline();
  const collection = mongoClient.db().collection(MONGO_COLLECTION);
  const aggOptions = { allowDiskUse: true };
  if (MONGO_MAX_TIME_MS > 0) aggOptions.maxTimeMS = MONGO_MAX_TIME_MS;
  const cursor = collection.aggregate(pipeline, aggOptions);

  const stopFlag = { value: false };
  let stopKind = null; // 'signal' | 'idle_timeout' | 'overall_timeout'
  let stopReason = null;

  const removeSignalHandlers = setupSignalHandlers(() => {
    stopFlag.value = true;
    stopKind = stopKind || 'signal';
    stopReason = stopReason || 'nhận tín hiệu dừng từ hệ thống (SIGINT/SIGTERM)';
  });

  let overallTimeoutTimer = null;
  if (MONGO_TIMEOUT_MS > 0) {
    overallTimeoutTimer = setTimeout(() => {
      stopFlag.value = true;
      stopKind = stopKind || 'overall_timeout';
      stopReason = stopReason || `vượt quá MONGO_TIMEOUT_SECS tổng = ${MONGO_TIMEOUT_MS / 1000}s`;
      cursor.close().catch(() => { });
    }, MONGO_TIMEOUT_MS);
  }

  const fetchCtx = makeFetchContext();
  let idleWatchdog = null;
  if (MONGO_IDLE_TIMEOUT_MS > 0) {
    idleWatchdog = setInterval(() => {
      if (!fetchCtx.idleTriggered && Date.now() - fetchCtx.lastDocTime > MONGO_IDLE_TIMEOUT_MS) {
        fetchCtx.idleTriggered = true;
        stopFlag.value = true;
        stopKind = stopKind || 'idle_timeout';
        stopReason = stopReason ||
          `không có dữ liệu mới từ MongoDB quá ${MONGO_IDLE_TIMEOUT_MS / 1000}s (nghi treo do thiếu index)`;
        log(`[watchdog] ${stopReason} -> huỷ cursor.`);
        cursor.close().catch(() => { });
      }
    }, Math.min(MONGO_IDLE_TIMEOUT_MS, IDLE_CHECK_INTERVAL_MS));
  }

  const clientRef = { current: await pgPool.connect() };
  await ensureStagingTable(clientRef.current);

  let totalCommitted = 0;
  let batchNum = 0;
  const overallStart = Date.now();

  try {
    while (true) {
      if (checkExternalStop() && !stopFlag.value) {
        stopFlag.value = true;
        stopKind = stopKind || 'signal';
        stopReason = stopReason || 'nhận yêu cầu dừng từ master thread';
      }

      const { batch, cursorDone } = await fetchBatch(cursor, fetchCtx, BATCH_SIZE, stopFlag);

      if (batch.length > 0) {
        batchNum++;
        await commitBatchWithRetry(pgPool, clientRef, batch, batchNum);
        totalCommitted += batch.length;
        const elapsedSec = ((Date.now() - overallStart) / 1000).toFixed(0);
        log(`[batch ${batchNum}] Đã commit ${batch.length} dòng (cộng dồn: ${totalCommitted}, ${elapsedSec}s).`);
      }

      if (cursorDone) break;
      if (stopFlag.value) break;
    }

    if (stopKind === 'idle_timeout' || stopKind === 'overall_timeout') {
      throw new Error(
        `Dừng do ${stopReason}. Đã ghi ổn định ${totalCommitted} dòng vào bảng đích trước khi dừng ` +
        `(không mất dữ liệu đã commit — chỉ cần chạy lại script là được, UPSERT sẽ tự bỏ qua phần trùng).`
      );
    }
    if (stopKind === 'signal') {
      log(`Dừng theo yêu cầu (${stopReason}). Đã ghi ổn định ${totalCommitted} dòng vào bảng đích.`);
      return totalCommitted;
    }

    log(
      `Hoàn tất fetch: tổng ${fetchCtx.count} dòng đọc từ MongoDB trong ` +
      `${((Date.now() - overallStart) / 1000).toFixed(0)}s.`
    );
    log(`Hoàn tất: đã đồng bộ ${totalCommitted} dòng vào bảng '${PG_TABLE_FQ}' qua ${batchNum} batch.`);
    return totalCommitted;
  } finally {
    if (overallTimeoutTimer) clearTimeout(overallTimeoutTimer);
    if (idleWatchdog) clearInterval(idleWatchdog);
    removeSignalHandlers();
    await cursor.close().catch(() => { });
    clientRef.current.release();
  }
}

// ================== MAIN ==================

// async function main() {
//   const pgPool = new Pool({
//     host: PG_HOST,
//     port: PG_PORT,
//     database: PG_DB,
//     user: PG_USER,
//     password: PG_PASSWORD,
//   });
//   const mongoClient = new MongoClient(MONGO_URI);

//   let exitCode = 0;
//   try {
//     await ensurePostgresSchema(pgPool);

//     await mongoClient.connect();
//     await pingMongo(mongoClient);

//     await runAggregationAndLoad(mongoClient, pgPool);
//   } catch (err) {
//     log('LỖI:', err && err.message ? err.message : err);
//     exitCode = 1;
//   } finally {
//     await mongoClient.close().catch(() => { });
//     await pgPool.end().catch(() => { });
//   }
//   process.exit(exitCode);
// }
// ================== MASTER: SPAWN & QUẢN LÝ WORKER ==================

// async function runAsMaster() {
//   // Bước 1: đảm bảo schema/bảng đích tồn tại — CHỈ chạy 1 lần ở master,
//   // trước khi spawn worker, để tránh N worker cùng chạy DDL song song
//   // (CREATE TABLE IF NOT EXISTS vẫn an toàn nếu race, nhưng làm 1 lần cho sạch).
//   const pgPoolForDdl = new Pool({
//     host: PG_HOST, port: PG_PORT, database: PG_DB, user: PG_USER, password: PG_PASSWORD,
//   });
//   try {
//     await ensurePostgresSchema(pgPoolForDdl);
//   } finally {
//     await pgPoolForDdl.end().catch(() => {});
//   }

//   // Kiểm tra kết nối Mongo 1 lần ở master cho nhanh phát hiện lỗi cấu hình
//   // (mỗi worker vẫn tự connect Mongo riêng bên trong).
//   const mongoPingClient = new MongoClient(MONGO_URI);
//   try {
//     await mongoPingClient.connect();
//     await pingMongo(mongoPingClient);
//   } finally {
//     await mongoPingClient.close().catch(() => {});
//   }

//   log(`Spawn ${WORKER_COUNT} worker thread để chạy song song...`);

//   let stopAllRequested = false;
//   const workers = [];

//   const spawnWorker = (i) =>
//     new Promise((resolve, reject) => {
//       const w = new Worker(__filename, {
//         workerData: { workerCount: WORKER_COUNT, workerIndex: i },
//         // Cho phép truyền code thoát khi worker exit
//         stdout: false,
//         stderr: false,
//       });
//       workers.push(w);

//       w.on('exit', (code) => {
//         if (code === 0) {
//           resolve({ index: i, ok: true });
//         } else {
//           resolve({ index: i, ok: false, code });
//         }
//       });
//       w.on('error', (err) => {
//         resolve({ index: i, ok: false, err });
//       });
//     });

//   // Forward Ctrl+C / SIGTERM từ master xuống toàn bộ worker để mỗi worker
//   // tự dừng an toàn theo đúng cơ chế "dừng sau khi batch hiện tại commit xong"
//   // đã có sẵn trong setupSignalHandlers() của từng worker.
//   const removeMasterSignalHandlers = setupSignalHandlers((sig) => {
//     if (stopAllRequested) {
//       log(`Nhận thêm tín hiệu ${sig} ở master — ép thoát toàn bộ worker.`);
//       for (const w of workers) w.terminate().catch(() => {});
//       process.exit(130);
//     }
//     stopAllRequested = true;
//     log(`Master nhận ${sig} — chuyển tiếp cho ${workers.length} worker để dừng an toàn.`);
//     for (const w of workers) {
//       // Worker cũng có process signal handler riêng nếu chạy standalone,
//       // nhưng trong worker_threads phải gửi message thay vì OS signal.
//       w.postMessage({ type: 'stop' });
//     }
//   });

//   try {
//     const results = await Promise.all(
//       Array.from({ length: WORKER_COUNT }, (_, i) => spawnWorker(i))
//     );

//     const failed = results.filter((r) => !r.ok);
//     if (failed.length > 0) {
//       for (const f of failed) {
//         log(
//           `[worker ${f.index + 1}/${WORKER_COUNT}] THẤT BẠI — ` +
//           (f.err ? f.err.message : `exit code ${f.code}`)
//         );
//       }
//       throw new Error(`${failed.length}/${WORKER_COUNT} worker chạy lỗi. Xem log phía trên.`);
//     }

//     log(`Tất cả ${WORKER_COUNT} worker đã hoàn tất thành công.`);
//   } finally {
//     removeMasterSignalHandlers();
//   }
// }
async function runAsMaster() {
  const pgPoolForDdl = new Pool({
    host: PG_HOST, port: PG_PORT, database: PG_DB, user: PG_USER, password: PG_PASSWORD,
  });
  try {
    await ensurePostgresSchema(pgPoolForDdl);
  } finally {
    await pgPoolForDdl.end().catch(() => {});
  }

  const mongoPingClient = new MongoClient(MONGO_URI);
  try {
    await mongoPingClient.connect();
    await pingMongo(mongoPingClient);
  } finally {
    await mongoPingClient.close().catch(() => {});
  }

  log(`Spawn ${WORKER_COUNT} worker thread để chạy song song...`);

  // Khai báo TRƯỚC khi setupSignalHandlers, để closure của handler
  // tham chiếu đúng các biến này (không phải bản "ma" ở scope khác).
  let stopAllRequested = false;
  const workers = [];
  const finalCountByWorker = new Map();
  const activeWorkers = new Set();
  const overallStart = Date.now();
  let lastGlobalLog = 0;

  function logGlobalProgress(force = false) {
    const now = Date.now();
    if (!force && now - lastGlobalLog < PROGRESS_INTERVAL_MS) return;
    lastGlobalLog = now;

    let total = 0;
    for (const c of finalCountByWorker.values()) total += c;

    const elapsedSec = (now - overallStart) / 1000;
    const rate = elapsedSec > 0 ? (total / elapsedSec).toFixed(0) : total;
    let line =
      `[TỔNG] ${total} dòng đã đọc từ MongoDB (${activeWorkers.size}/${WORKER_COUNT} worker đang chạy) ` +
      `- ${elapsedSec.toFixed(0)}s - ~${rate} dòng/s`;
    if (TOTAL_DOCS_HINT > 0) {
      const pct = ((total / TOTAL_DOCS_HINT) * 100).toFixed(1);
      line += ` - ~${pct}%`;
    }
    log(line);
  }

  const spawnWorker = (i) =>
    new Promise((resolve) => {
      const w = new Worker(__filename, {
        workerData: { workerCount: WORKER_COUNT, workerIndex: i },
      });
      workers.push(w);
      activeWorkers.add(i);

      w.on('message', (msg) => {
        if (msg && msg.type === 'progress') {
          finalCountByWorker.set(i, msg.count);
          logGlobalProgress();
        }
      });

      w.on('exit', (code) => {
        activeWorkers.delete(i);
        logGlobalProgress(true);
        resolve(code === 0 ? { index: i, ok: true } : { index: i, ok: false, code });
      });
      w.on('error', (err) => {
        activeWorkers.delete(i);
        resolve({ index: i, ok: false, err });
      });
    });

  // Đăng ký DUY NHẤT 1 lần ở đây, dùng closure đúng workers/stopAllRequested
  // vừa khai báo ở trên. Đây là chỗ DUY NHẤT gọi setupSignalHandlers trong
  // toàn bộ luồng master.
  const removeMasterSignalHandlers = setupSignalHandlers((sig) => {
    if (stopAllRequested) {
      log(`Nhận thêm tín hiệu ${sig} ở master — ép thoát toàn bộ worker.`);
      for (const w of workers) w.terminate().catch(() => {});
      process.exit(130);
    }
    stopAllRequested = true;
    log(`Master nhận ${sig} — chuyển tiếp cho ${workers.length} worker để dừng an toàn.`);
    for (const w of workers) {
      w.postMessage({ type: 'stop' });
    }
  });

  try {
    const results = await Promise.all(
      Array.from({ length: WORKER_COUNT }, (_, i) => spawnWorker(i))
    );

    logGlobalProgress(true);

    const failed = results.filter((r) => !r.ok);
    if (failed.length > 0) {
      for (const f of failed) {
        log(
          `[worker ${f.index + 1}/${WORKER_COUNT}] THẤT BẠI — ` +
          (f.err ? f.err.message : `exit code ${f.code}`)
        );
      }
      throw new Error(`${failed.length}/${WORKER_COUNT} worker chạy lỗi. Xem log phía trên.`);
    }

    let grandTotal = 0;
    for (const c of finalCountByWorker.values()) grandTotal += c;
    log(`Tất cả ${WORKER_COUNT} worker đã hoàn tất thành công. Tổng cộng đã đọc/đồng bộ ~${grandTotal} dòng.`);
  } finally {
    // CHỈ gỡ 1 lần duy nhất, sau khi mọi worker đã xong (hoặc lỗi).
    removeMasterSignalHandlers();
  }
}

// ================== WORKER: CHẠY SYNC CHO 1 PARTITION ==================

// async function runAsWorker() {
//   const pgPool = new Pool({
//     host: PG_HOST, port: PG_PORT, database: PG_DB, user: PG_USER, password: PG_PASSWORD,
//   });
//   const mongoClient = new MongoClient(MONGO_URI);

//   // Nhận tín hiệu "stop" từ master qua postMessage (worker_threads không có
//   // OS signal riêng cho từng thread, nên forward qua message thay vì SIGINT/SIGTERM).
//   let stopFromMaster = false;
//   parentPort.on('message', (msg) => {
//     if (msg && msg.type === 'stop') {
//       stopFromMaster = true;
//     }
//   });

//   let exitCode = 0;
//   try {
//     await mongoClient.connect();
//     await pingMongo(mongoClient);
//     await runAggregationAndLoad(mongoClient, pgPool, () => stopFromMaster);
//   } catch (err) {
//     log('LỖI:', err && err.message ? err.message : err);
//     exitCode = 1;
//   } finally {
//     await mongoClient.close().catch(() => {});
//     await pgPool.end().catch(() => {});
//   }
//   process.exit(exitCode);
// }
async function runAsWorker() {
  const pgPool = new Pool({
    host: PG_HOST, port: PG_PORT, database: PG_DB, user: PG_USER, password: PG_PASSWORD,
  });
  const mongoClient = new MongoClient(MONGO_URI);

  let stopFromMaster = false;
  parentPort.on('message', (msg) => {
    if (msg && msg.type === 'stop') stopFromMaster = true;
  });

  let exitCode = 0;
  try {
    await mongoClient.connect();
    await pingMongo(mongoClient);
    await runAggregationAndLoad(mongoClient, pgPool, () => stopFromMaster);
  } catch (err) {
    log('LỖI:', err && err.message ? err.message : err);
    exitCode = 1;
  } finally {
    await mongoClient.close().catch(() => { });
    await pgPool.end().catch(() => { });
  }
  process.exit(exitCode);
}

// ================== MAIN ==================

async function main() {
  if (isMainThread) {
    let exitCode = 0;
    try {
      await runAsMaster();
    } catch (err) {
      log('LỖI:', err && err.message ? err.message : err);
      exitCode = 1;
    }
    process.exit(exitCode);
  } else {
    await runAsWorker();
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  docToTsvLine,
  escTsv,
  unwrapIdLike,
  toTimestampIso,
  commitBatch,
};