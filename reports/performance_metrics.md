# Performance Evaluation Report

## 1. Overview

This report measures the Real-Time Data Ingestion Pipeline (Spark Structured Streaming to PostgreSQL) in two controlled runs on a local Windows machine, 2026-11-06:

| Run | Generator settings | Offered rate | Purpose |
|---|---|---|---|
| Baseline | 10 events per file, 1 file / 5 s, 12 files | about 2 events/s | The normal configuration |
| Load | 500 events per file, 1 file / 1 s, 40 files | about 500 events/s | Does the pipeline keep up under heavier input? |

Each run used a fresh PostgreSQL table, a fresh checkpoint and its own input folder, and was measured with `scripts/measure_performance.py`. The raw per-batch data is in `reports/metrics_baseline_run.csv` and `reports/metrics_load_run.csv`.

Spark settings: trigger every 5 seconds, up to 10 files per micro-batch, local mode.

## 2. Results

| Metric | Baseline | Load |
|---|---:|---:|
| Events generated | 120 | 20,000 |
| Events inserted into PostgreSQL | 120 | 20,000 |
| Events rejected | 0 | 0 |
| Micro-batches | 12 | 9 |
| Batch duration, average | 0.610 s | 0.731 s |
| Batch duration, minimum | 0.319 s | 0.229 s |
| Batch duration, maximum | 2.674 s | 3.072 s |
| Batch duration, p95 | 0.545 s | 3.072 s |
| Processing capacity (records per second of batch time) | 16.4 | 3,039.5 |
| End-to-end throughput (records per second of wall-clock time) | 2.2 | 509.6 |
| Latency, average | 2.51 s | 4.49 s |
| Latency, p95 | 6.36 s | 7.61 s |
| Latency, maximum | 6.36 s | 9.39 s |
| Backlog at the end | none | none |

## 3. How the metrics are defined

- **Latency** is `ingestion_time - event_time` for every stored row. `event_time` is set when the generator creates the event and only has 1-second resolution, so each value is accurate to about 1 second. It covers the whole path: file write, waiting for the next trigger, Spark processing and the database insert.
- **End-to-end throughput** is the number of records processed after the first batch divided by the wall-clock time between the first and last batch. It reflects what the pipeline actually delivered while data was arriving.
- **Processing capacity** is records divided by the total time spent inside batches. It shows how fast a batch is processed once Spark has it, not the rate at which data flows through the system, so it is much higher than throughput whenever the pipeline sits idle between triggers.

## 4. Analysis

**Correctness under load.** Every generated event reached PostgreSQL in both runs (120 of 120, 20,000 of 20,000), with no failed batches and no duplicates.

**Keeping up.** In the load run the pipeline delivered 509.6 events/s against an offered rate of about 500 events/s and finished with no backlog, so it kept pace with the input. In the baseline run the throughput of 2.2 events/s matches the generator's 2 events/s.

**Batch time.** Average batch duration stayed well below the 5-second trigger interval in both runs (0.61 s and 0.73 s), which leaves room for more load. The maximum values (2.7 s and 3.1 s) are the first batch of each run, which pays for Spark's one-time warm-up. After that, batches took 0.32 to 0.55 s in the baseline run and 0.23 to 0.97 s in the load run (typically 0.35 to 0.45 s for 2,000 to 2,500 records), so batch time barely grew with batch size. With only 9 and 12 batches, the p95 batch duration is effectively the second-slowest or slowest batch and should be read loosely.

**Latency.** Average latency was 2.5 s in the baseline and 4.5 s under load. Latency comes mostly from waiting for the 5-second trigger plus the batch time. Under load a file arrives every second, so each batch collected 4 to 6 files and the earliest ones in a batch waited most of a trigger interval; that waiting, not processing time, explains the higher latency. Lowering `TRIGGER_INTERVAL` would reduce it. The p95 latency stayed below 10 s in both runs.

**What the load run did not test.** The load run reached about 500 events/s. It did not find the pipeline's maximum, because the generator was the limit. A higher rate, more files per trigger or a bigger machine would be needed to find where it starts to fall behind.

## 5. Earlier results and a configuration change

The previous version of this report used the live metrics file `reports/streaming_metrics.csv`, which only contains batches 43 to 103 of a longer session (the database holds 1,050 events from 105 files). Its "40.73 records/s" figure was processing capacity (records divided by batch time) and was described as throughput, which overstated how fast data actually moved: the generator produced only 2 events/s. That figure has been replaced by the two separate measures above.

That session also ran with `maxFilesPerTrigger = 1`. With a 5-second trigger this limits the pipeline to one file per 5 seconds, so by design it cannot keep up with any input faster than one file per trigger. The setting is now configurable and defaults to 10 files per trigger (`MAX_FILES_PER_TRIGGER` in `.env`). This limit was worked out from the configuration and was not measured with the old setting.

## 6. Limitations

- Both runs used one machine that also generated the data, so the generator and Spark competed for the same CPU.
- Local mode (`local[*]`) only; no cluster.
- The load run lasted about 40 seconds. A longer soak test would show whether latency stays stable.
- Each batch opens a new database connection and collects its rows to the Spark driver. That is fine at these sizes but would be the first thing to change for much larger batches.
- Latency has a resolution of about 1 second because of the `event_time` format.

## 7. Reproducing the measurements

```bash
# terminal 1
python scripts/spark_streaming_to_postgres.py

# terminal 2 (baseline: defaults; load: more events, shorter interval)
python scripts/data_generator.py --events 500 --interval 1 --files 40

# afterwards
python scripts/measure_performance.py --since "YYYY-MM-DD HH:MM:SS"
```

To keep a measured run separate from your real data, set `POSTGRES_DB`, `INPUT_DIR`, `CHECKPOINT_DIR` and `METRICS_FILE` in the environment before starting both scripts.
