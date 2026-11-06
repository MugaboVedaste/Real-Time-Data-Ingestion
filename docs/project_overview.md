# Real-Time Data Ingestion Pipeline

## 1. Project Overview

The Real-Time Data Ingestion Pipeline is a data engineering project designed to collect, process, validate, and store simulated e-commerce events using Apache Spark Structured Streaming and PostgreSQL.

The system continuously monitors an incoming data directory for new CSV files. Apache Spark reads these files, processes the events in micro-batches, applies data validation and transformation, and sends the processed records to PostgreSQL.

The pipeline also maintains checkpoints to track processing progress and records performance metrics to evaluate its operation.

## 2. Project Objectives

The main objectives are:

- Build a real-time data ingestion pipeline.
- Generate simulated e-commerce events.
- Process incoming CSV files using Spark Structured Streaming.
- Validate and transform incoming event data.
- Store processed events in PostgreSQL.
- Prevent duplicate event insertion.
- Implement checkpointing for streaming progress and recovery.
- Monitor batch processing performance.

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Event generation and pipeline implementation |
| Apache Spark | Distributed data processing |
| Spark Structured Streaming | Continuous micro-batch processing |
| PostgreSQL | Persistent data storage |
| Pandas / CSV | Data inspection and metrics reporting |
| psycopg2 | PostgreSQL connectivity |
| python-dotenv | Environment configuration |
| Git and GitHub | Version control |

## 4. Main Features

1. **Event Generation:** Generates simulated e-commerce events, including views, add-to-cart actions, and purchases.
2. **Streaming Ingestion:** Detects and processes newly generated CSV files.
3. **Data Validation:** Filters invalid records and checks required fields and event types.
4. **Database Integration:** Inserts processed events into PostgreSQL.
5. **Duplicate Prevention:** Uses event IDs and PostgreSQL conflict handling to avoid duplicate records.
6. **Checkpointing:** Maintains streaming progress to support recovery.
7. **Performance Monitoring:** Records batch processing statistics in a CSV report.

## 5. Data Flow

1. The Python generator creates e-commerce event records.
2. Events are saved as CSV files in the incoming data directory.
3. Spark Structured Streaming detects new files.
4. Spark reads, transforms, and validates the records.
5. The foreachBatch function processes each micro-batch.
6. Valid records are inserted into PostgreSQL.
7. Processing metrics are recorded for performance evaluation.

## 6. Expected Outcome

The expected outcome is a functional streaming pipeline that continuously ingests simulated e-commerce events, validates and processes them, and stores them in PostgreSQL while tracking processing progress and performance.