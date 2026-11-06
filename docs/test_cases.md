# Test Plan and Results – Real-Time Data Ingestion Pipeline

## 1. Introduction

This document is the test plan for the pipeline. It has two parts:

- **Manual tests (MT)** – run against the live pipeline, each with an expected and an actual outcome.
- **Automated tests (AT)** – pytest suite in `tests/`.

Together they answer the five questions in the project brief: are CSVs generated correctly, does Spark detect new files, are the transformations correct, is data written to PostgreSQL without errors, and is performance within expected limits.

## 2. Test Environment

| Component | Configuration |
|---|---|
| OS | Windows 11 |
| Python / PySpark | 3.13 / 4.0.1 (local mode, `local[*]`) |
| Java | 17 |
| Database | PostgreSQL, database `ecommerce_streaming` (manual tests ran on a throwaway copy, `ecommerce_perf_test`, created from `sql/postgres_setup.sql` and dropped afterwards) |
| Spark settings | trigger every 5 s, `maxFilesPerTrigger=10` |
| Test date | 2026-11-06 |

## 3. Manual Test Plan

| ID | Area | Steps | Expected | Actual | Result |
|---|---|---|---|---|---|
| MT-01 | CSV generation | Run `python scripts/data_generator.py --events 10 --interval 5 --files 12` | 12 CSV files in the input folder, each with the header `event_id,user_id,product_id,event_type,price,event_time` and 10 rows; no temp files left behind | 12 files created, 10 rows each, correct header, no `.tmp` files | PASS |
| MT-02 | File detection | Start Spark first, then the generator (12 files, one every 5 s) | One micro-batch per new file, within about one trigger interval of the file appearing | 12 batches for 12 files (batch IDs 0–11), each with 10 processed records | PASS |
| MT-03 | Transformation / validation | Drop a CSV with 2 valid rows and 5 invalid rows (unsupported type `refund`, negative price, zero price, malformed timestamp `not-a-date`, missing `user_id`) | 2 rows inserted, 5 rejected, stream keeps running | Log: `Batch 12: rejected 5 invalid record(s) out of 7`. Database contains exactly the 2 valid event IDs. Stream kept running | PASS |
| MT-04 | Duplicate protection | Copy an already-ingested CSV into the input folder under a new name | Records are read but 0 inserted, no duplicate IDs | `Processed: 10 \| Inserted: 0`. `COUNT(*) - COUNT(DISTINCT event_id)` = 0 | PASS |
| MT-05 | Database writes | Compare records read by Spark with rows in PostgreSQL after the baseline run | Row count matches generated events, no errors in the Spark log | 120 generated, 120 inserted (baseline); 20,000 generated, 20,000 inserted (load run). No errors | PASS |
| MT-06 | Performance limits | Baseline (10 events / 5 s) and load (500 events / 1 s) runs, see `reports/performance_metrics.md` | Average batch time below the 5 s trigger, pipeline keeps up with the input rate (no backlog), p95 latency under 10 s | Baseline: avg batch 0.61 s, p95 latency 6.4 s. Load: avg batch 0.73 s, p95 latency 7.6 s. Both finished with no backlog | PASS |
| MT-07 | Database constraints | Insert rows with price `-1`, price `0`, type `refund` and one valid row into a table created from `postgres_setup.sql` | The three invalid rows are rejected by `CHECK` constraints, the valid row is accepted | `-1` and `0` rejected (`user_events_price_check`), `refund` rejected (`user_events_event_type_check`), valid row accepted | PASS |

## 4. Automated Tests

Run:

```bash
python -m pytest tests/ -v
```

| ID | File | What it checks | Needs |
|---|---|---|---|
| AT-01 | `test_pipeline.py` | Database connection succeeds | PostgreSQL |
| AT-02 | `test_pipeline.py` | `user_events` contains records | PostgreSQL with data |
| AT-03 | `test_pipeline.py` | No duplicate event IDs | PostgreSQL |
| AT-04 | `test_pipeline.py` | Only `view`, `add_to_cart`, `purchase` stored | PostgreSQL |
| AT-05 | `test_pipeline.py` | All stored prices are positive | PostgreSQL |
| AT-06 | `test_pipeline.py` | No NULLs in required columns | PostgreSQL |
| AT-07 | `test_generator.py` | CSV has the expected header and row count | – |
| AT-08 | `test_generator.py` | Generated values are valid (type, price, user range, no blanks) | – |
| AT-09 | `test_generator.py` | Event IDs are unique across files | – |
| AT-10 | `test_generator.py` | No temporary files left after writing (atomic write) | – |
| AT-11 | `test_transformations.py` | A valid row is kept and converted to a timestamp / decimal | Java, PySpark |
| AT-12 | `test_transformations.py` | All three event types are kept | Java, PySpark |
| AT-13 | `test_transformations.py` | 11 kinds of invalid row are each rejected (missing fields, bad type, zero / negative / non-numeric price, bad / missing timestamp) | Java, PySpark |
| AT-14 | `test_transformations.py` | A mixed batch keeps only the valid rows | Java, PySpark |

**Actual result:** 24 tests collected (AT-13 runs as 11 cases), **24 passed** in about 15 seconds.

The database tests (AT-01 to AT-06) check the state of the data after the pipeline has run. AT-07 to AT-14 test the generator and the cleaning logic directly, using deliberately bad data, so they do not depend on what happens to be in the database.

## 5. Defect Found During Testing

While writing AT-13 the malformed-timestamp case crashed the job. Spark 4 runs in ANSI mode, where `to_timestamp` raises an error on an unparseable value instead of returning NULL, so a single bad timestamp would have stopped the stream. The pipeline now uses `try_to_timestamp`, which returns NULL so the row is rejected normally. MT-03 confirms the fixed behaviour end to end.

## 6. Conclusion

All seven manual tests and the 24 automated tests passed. The pipeline detects new files, rejects invalid records, ignores replayed duplicates and writes valid records to PostgreSQL without errors.
