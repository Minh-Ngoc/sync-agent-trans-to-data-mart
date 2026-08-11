# MongoDB → PostgreSQL Sync Worker

## 1. Overview

This script is a data synchronization pipeline that reads data from MongoDB, transforms and partitions it through a MongoDB aggregation pipeline, processes the result in parallel workers, and writes the data to PostgreSQL in batches.

```text
MongoDB
   │
   │ Aggregation Pipeline + Partitioning
   ▼
MongoDB Cursor
   │
   ▼
Multiple Workers
   │
   │ Batch
   ▼
PostgreSQL TEMP Staging
   │
   │ COPY
   ▼
Deduplicate
   │
   │ UPSERT
   ▼
PostgreSQL Target
   │
   ▼
COMMIT
```

The design provides:

- MongoDB as the source.
- PostgreSQL as the destination.
- MongoDB-side filtering/transformation.
- Parallel processing with multiple workers.
- Cursor-based streaming instead of loading the full dataset.
- Batch-based PostgreSQL writes.
- Bulk `COPY` into a staging table.
- Deduplication before `UPSERT`.
- Batch-level transactions.
- PostgreSQL retry handling.
- MongoDB cursor idle detection.
- Worker stop signaling and resource cleanup.

---

## 2. Architecture

The script uses a master/worker architecture.

```text
                         MASTER
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
         Worker 0      Worker 1      Worker 2 ... Worker N
             │             │             │
             └─────────────┼─────────────┘
                           │
                     PostgreSQL
```

### Master responsibilities

- Prepare the execution environment.
- Create and manage workers.
- Monitor worker lifecycle.
- Send stop messages when necessary.

### Worker responsibilities

Each worker:

1. Creates a PostgreSQL pool.
2. Creates a MongoDB client.
3. Registers the stop-message handler.
4. Connects to MongoDB.
5. Pings MongoDB.
6. Runs the aggregation pipeline.
7. Reads its assigned partition through a cursor.
8. Builds batches.
9. Loads batches into PostgreSQL staging.
10. Deduplicates records.
11. Performs the PostgreSQL `UPSERT`.
12. Commits successful batches.
13. Retries PostgreSQL write failures when configured.
14. Cleans up database connections.

---

## 3. Master Process

Workers are created with:

```js
workerData: {
    workerCount: WORKER_COUNT,
    workerIndex: i
}
```

Therefore every worker knows:

- The total number of workers.
- Its own worker index.

For example:

```text
WORKER_COUNT = 4

Worker 0 → workerIndex = 0
Worker 1 → workerIndex = 1
Worker 2 → workerIndex = 2
Worker 3 → workerIndex = 3
```

The master does not perform the main MongoDB-to-PostgreSQL data processing itself. The synchronization work happens inside workers.

---

## 4. Worker Lifecycle

```text
runAsWorker()
    │
    ├── Create PostgreSQL Pool
    ├── Create MongoDB Client
    ├── Register stop-message handler
    ├── Connect to MongoDB
    ├── Ping MongoDB
    ├── Run aggregation + load
    ├── Handle errors
    └── Close MongoDB/PostgreSQL connections
```

The worker starts the synchronization only after MongoDB connection and ping succeed.

---

## 5. Data Partitioning

Records are partitioned using:

```text
transid % WORKER_COUNT
```

For four workers:

```text
transid % 4 = 0 → Worker 0
transid % 4 = 1 → Worker 1
transid % 4 = 2 → Worker 2
transid % 4 = 3 → Worker 3
```

Example:

```text
transid 100 → Worker 0
transid 101 → Worker 1
transid 102 → Worker 2
transid 103 → Worker 3
transid 104 → Worker 0
```

The purpose is to allow workers to process independent portions of the dataset concurrently.

### Important rule

The partitioning logic depends on `WORKER_COUNT`.

```text
WORKER_COUNT = 4
→ transid % 4

WORKER_COUNT = 8
→ transid % 8
```

Therefore changing the worker count changes the partition boundaries.

---

## 6. MongoDB Aggregation

The worker does not retrieve the entire collection into Node.js.

The MongoDB aggregation pipeline performs filtering and transformation before the result reaches the worker.

Conceptually:

```text
MongoDB Collection
       │
       ▼
Aggregation stages
       │
       ▼
Partition filter
       │
       ▼
Projection / transformation
       │
       ▼
MongoDB Cursor
```

The worker consumes the aggregation result through the cursor.

---

## 7. Cursor-Based Processing

The script uses a MongoDB cursor:

```text
MongoDB Cursor

Document 1
Document 2
Document 3
...
Document N
```

Documents are consumed incrementally and added to the current batch.

This avoids intentionally materializing the entire source dataset in Node.js memory.

---

## 8. Batch Processing

Records are processed in batches.

For example:

```text
BATCH_SIZE = 10,000
```

Execution becomes:

```text
10,000 records → Batch 1
10,000 records → Batch 2
10,000 records → Batch 3
...
```

Each batch follows the same write path:

```text
Batch N
   │
   ├── Prepare staging
   ├── COPY
   ├── Deduplicate
   ├── UPSERT
   └── COMMIT
```

---

## 9. PostgreSQL Staging

The worker does not insert every MongoDB record directly into the target table.

Instead:

```text
MongoDB Batch
      │
      ▼
TEMP staging table
      │
      ▼
Deduplicate
      │
      ▼
Target table
```

The staging table acts as an intermediate bulk-load area.

---

## 10. PostgreSQL COPY

The staging table is populated using PostgreSQL `COPY`.

Instead of executing:

```text
INSERT row 1
INSERT row 2
INSERT row 3
...
INSERT row 10000
```

the worker uses:

```text
Node.js Batch
      │
      ▼
COPY FROM STDIN
      │
      ▼
staging table
```

This is intended to reduce per-row SQL overhead during large synchronization jobs.

---

## 11. Duplicate Handling

A batch can contain multiple records with the same `agent_trans_id`.

The script therefore deduplicates staging data before the final `UPSERT`.

Example:

```text
staging

A001 → row 1
A001 → row 2
A001 → row 3
A002 → row 4
```

becomes:

```text
A001 → one selected row
A002 → one selected row
```

The deduplication logic uses:

```sql
DISTINCT ON (agent_trans_id)
```

before the final write.

This prevents PostgreSQL from trying to update the same target row multiple times within one `INSERT ... ON CONFLICT DO UPDATE` operation.

---

## 12. PostgreSQL UPSERT

After staging and deduplication, the worker writes to the target table using an UPSERT pattern:

```text
INSERT
  │
  ├── No conflict
  │      └── INSERT
  │
  └── Conflict
         └── UPDATE
```

Conceptually:

```sql
INSERT INTO target (...)
SELECT ...
FROM staging_sync
...
ON CONFLICT (agent_trans_id)
DO UPDATE ...
```

Therefore existing target rows can be updated while new rows are inserted.

---

## 13. Transaction Boundary

Transactions are scoped to individual batches rather than the entire synchronization.

```text
Batch 1
BEGIN
  COPY
  UPSERT
COMMIT

Batch 2
BEGIN
  COPY
  UPSERT
COMMIT

Batch 3
BEGIN
  COPY
  UPSERT
COMMIT
```

If Batch 3 fails:

```text
Batch 1 → committed
Batch 2 → committed
Batch 3 → failed
```

Previously committed batches are not rolled back.

### Important consequence

The script provides **batch-level durability**, not one atomic transaction covering the complete MongoDB dataset.

---

## 14. PostgreSQL Retry

PostgreSQL writes can be retried using:

```text
PG_MAX_RETRIES
PG_RETRY_DELAY_SECS
```

Conceptually:

```text
commitBatch()
     │
     ▼
PostgreSQL
     │
 ┌───┴────┐
 │        │
OK      ERROR
 │        │
 ▼        ▼
COMMIT   RETRY
           │
           ├── Attempt 1
           ├── Attempt 2
           ├── Attempt 3
           └── ...
```

This is intended to tolerate transient PostgreSQL failures.

---

## 15. MongoDB Idle Watchdog

The script monitors MongoDB cursor inactivity through:

```text
MONGO_IDLE_TIMEOUT_MS
```

Normal flow:

```text
Read document
     │
     ▼
Activity detected
     │
     ▼
Read next document
     │
     ▼
...
```

If the cursor produces no new document within the configured timeout:

```text
No progress
    │
    ▼
Idle timeout
    │
    ▼
Worker reports an error / stops processing
```

The purpose is to prevent a stalled cursor from leaving a worker alive indefinitely without progress.

---

## 16. LIMIT_DOCS

The script supports limiting the number of documents for testing.

Example:

```text
LIMIT_DOCS=1000
```

This allows a small test execution before processing a large dataset.

Useful validation areas include:

- MongoDB connectivity.
- Aggregation correctness.
- Partitioning.
- Data mapping.
- PostgreSQL connectivity.
- `COPY`.
- Deduplication.
- UPSERT.
- Transaction behavior.

---

## 17. Worker Stop Signal

Workers listen for messages from the master through `parentPort`.

Conceptually:

```text
MASTER
   │
   │ { type: "stop" }
   ▼
WORKER
```

The worker records the stop request:

```text
stopFromMaster = true
```

The processing loop can then use this state to stop further work.

---

## 18. Error Handling

The worker wraps its main execution in `try/catch/finally`.

```text
try
 │
 ├── Connect MongoDB
 ├── Ping MongoDB
 └── Run synchronization
 │
 ├───────────────┐
 │               │
 ▼               ▼
SUCCESS         ERROR
 │               │
 ▼               ▼
continue        log error
 │               │
 │               ▼
 │           exitCode = 1
 │               │
 └───────┬───────┘
         ▼
       finally
         │
         ├── Close MongoDB
         └── Close PostgreSQL
```

An unrecoverable worker error results in a non-zero exit code.

---

## 19. Connection Cleanup

The worker closes its database resources during cleanup:

```text
MongoClient
    │
    ▼
close()

PostgreSQL Pool
    │
    ▼
end()
```

This applies to both successful and failed executions.

---

## 20. Complete Runtime Flow

```text
START
  │
  ▼
main()
  │
  ├── Master process
  │      │
  │      ├── Prepare PostgreSQL
  │      ├── Prepare worker configuration
  │      └── Spawn N workers
  │
  └── Worker process
         │
         ▼
    runAsWorker()
         │
         ├── Create PG Pool
         ├── Create Mongo Client
         ├── Register stop listener
         │
         ▼
    MongoDB connect
         │
         ▼
      Mongo ping
         │
         ▼
    Build aggregation
         │
         ▼
    Apply partition
         │
         ▼
    MongoDB cursor
         │
         ▼
    Read documents
         │
         ▼
    Build batch
         │
         ▼
    Prepare staging
         │
         ▼
    COPY batch → staging
         │
         ▼
    Deduplicate staging
         │
         ▼
    UPSERT target
         │
         ▼
      COMMIT
         │
         ▼
    Next batch
         │
         ├───────────────┐
         │               │
      More data       No more data
         │               │
         └─── loop       ▼
                       Cleanup
                          │
                          ▼
                         END
```

---

## 21. Core Rules

| # | Rule | Purpose |
|---|---|---|
| 1 | Partition using `transid % WORKER_COUNT` | Distribute work across workers |
| 2 | Perform filtering/transformation in MongoDB | Reduce work transferred to Node.js |
| 3 | Consume MongoDB through a cursor | Avoid loading the full dataset into memory |
| 4 | Process records in batches | Control memory and transaction size |
| 5 | Bulk-load with PostgreSQL `COPY` | Improve write throughput |
| 6 | Use a staging table | Separate bulk loading from target writes |
| 7 | Deduplicate by `agent_trans_id` | Prevent duplicate conflict operations |
| 8 | UPSERT into the target | Insert new rows and update existing rows |
| 9 | Commit per batch | Preserve successful progress |
| 10 | Retry PostgreSQL writes | Handle transient database errors |
| 11 | Watch MongoDB cursor inactivity | Detect stalled processing |
| 12 | Support worker stop messages | Allow controlled shutdown |
| 13 | Always close database resources | Avoid connection leaks |

---

## 22. Operational Characteristics

### Parallelism

```text
WORKER_COUNT = N
```

means up to N workers can process independent partitions concurrently.

Increasing workers also increases:

- MongoDB concurrency.
- PostgreSQL concurrency.
- Network traffic.
- CPU usage.
- PostgreSQL connection usage.
- Potential target/index contention.

Therefore `WORKER_COUNT` should be tuned against the actual MongoDB and PostgreSQL capacity.

### Memory

Memory usage is primarily influenced by:

```text
BATCH_SIZE
```

Larger batches generally mean:

- More rows per transaction.
- More memory usage.
- Potentially better bulk-write efficiency.

Smaller batches generally mean:

- Smaller transactions.
- Lower memory usage.
- More commits.
- Potentially more overhead.

### Durability

Successful batches are committed independently:

```text
Batch 1 ✓
Batch 2 ✓
Batch 3 ✓
Batch 4 ✗
```

The first three batches remain committed even when Batch 4 fails.

---

## 23. Failure Model

The main failure boundaries are:

```text
MongoDB connection failure
        ↓
Worker failure

MongoDB cursor stalls
        ↓
Idle watchdog
        ↓
Worker failure

PostgreSQL transient write failure
        ↓
Retry

PostgreSQL persistent write failure
        ↓
Batch / worker failure

Worker failure
        ↓
Master detects worker lifecycle failure
```

The important distinction is:

```text
Transient PostgreSQL error
        ↓
Retry

Unrecoverable error
        ↓
Worker exits
```

---

## 24. Design Summary

The script is a:

> **Parallel, cursor-based MongoDB → PostgreSQL batch synchronization pipeline with MongoDB-side aggregation, partitioned workers, PostgreSQL staging/COPY, deduplication, UPSERT, batch-level transactions, retry handling, idle detection, and controlled worker shutdown.**

Main data path:

```text
MongoDB
  ↓
Aggregation
  ↓
Partition
  ↓
Cursor
  ↓
Worker
  ↓
Batch
  ↓
PostgreSQL COPY
  ↓
Staging
  ↓
Deduplicate
  ↓
UPSERT
  ↓
COMMIT
```

Main scalability mechanism:

```text
                    MongoDB
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
          Worker 0  Worker 1  Worker 2 ... Worker N
             │         │         │
             └─────────┼─────────┘
                       ▼
                  PostgreSQL
```

Main reliability mechanisms:

```text
Batch transaction
      +
PostgreSQL retry
      +
MongoDB idle watchdog
      +
Worker error handling
      +
Connection cleanup
```
