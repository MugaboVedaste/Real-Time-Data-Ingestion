# Performance Evaluation Report

## 1. Overview

This report evaluates the performance of the Real-Time Data Ingestion Pipeline built using Apache Spark Structured Streaming and PostgreSQL.

The evaluation uses actual batch-processing metrics collected during pipeline execution.

## 2. Performance Results

| Metric | Result |
|---|---:|
| Total batches recorded | 61 |
| Total records processed | 620 |
| Records inserted into PostgreSQL | 620 |
| Average batch processing time | 0.250 seconds |
| Minimum batch processing time | 0.121 seconds |
| Maximum batch processing time | 1.456 seconds |
| Approximate processing throughput | 40.73 records/second |

## 3. Analysis

### Data Processing

The pipeline processed 620 records across 61 batches. All 620 records were successfully inserted into PostgreSQL according to the recorded metrics.

### Processing Time

The average batch processing duration was approximately 0.250 seconds. The fastest batch completed in 0.121 seconds, while the slowest took 1.456 seconds.

### Throughput

The approximate processing throughput was 40.73 records per second, calculated using the total number of processed records divided by the cumulative recorded batch-processing duration.

### Database Integration

All records reported as processed in the metrics were successfully inserted into PostgreSQL. This indicates that the Spark-to-PostgreSQL integration worked correctly during the measured period.

## 4. Conclusion

The performance evaluation demonstrates that the pipeline successfully processed and inserted streaming records into PostgreSQL.

The recorded batch durations indicate that the pipeline handled the tested workload efficiently. Further testing with larger datasets and higher event generation rates would help evaluate scalability and performance under heavier workloads.

## 5. Limitations

- The evaluation covers the recorded 61 batches, not necessarily the entire lifetime of the pipeline.
- The throughput calculation measures processing time and does not represent end-to-end event latency.
- The results were collected from a local development environment.
- Larger workloads are required for a more comprehensive scalability assessment.