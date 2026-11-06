# User Guide – Real-Time Data Ingestion Pipeline

## 1. Introduction

This guide explains how to install, configure, and run the Real-Time Data Ingestion Pipeline using Python, Apache Spark Structured Streaming, and PostgreSQL.

## 2. Prerequisites

Before running the project, ensure you have installed:

- Python 3.10 or later
- Java 17 or later
- Apache Spark (installed through the PySpark dependency)
- PostgreSQL
- Git
- Hadoop Windows utilities (`winutils.exe`) when running Spark on Windows

## 3. Project Setup

Clone the repository:

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Navigate into the project:

```bash
cd Real-Time-Data-Ingestion
```

## 4. Virtual Environment Setup

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Note: Use the Python environment configured for PySpark if your installation uses a separate virtual environment.

## 5. Database Configuration

1. Start PostgreSQL.
2. Create a database named `ecommerce_streaming`:

   ```bash
   psql -U postgres -c "CREATE DATABASE ecommerce_streaming;"
   ```

3. Create the table (and its constraints and indexes):

   ```bash
   psql -U postgres -d ecommerce_streaming -f sql/postgres_setup.sql
   ```

4. Copy `.env.example` to `.env` and fill in the connection variables
   (see also `postgres_connection_details.txt`).

Example configuration:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=ecommerce_streaming
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

Do not commit your actual `.env` file or database password to GitHub.

## 6. Hadoop Configuration on Windows

Ensure `winutils.exe` is available. By default the script looks in:

```text
D:\hadoop\bin\winutils.exe
```

If it is somewhere else, set `HADOOP_HOME` (the folder that contains `bin\winutils.exe`) in `.env` or your shell. The script only applies this on Windows.

## 7. Running the Pipeline

### Terminal 1: Start Spark Streaming

Activate the configured Python environment, navigate to the project directory and run:

```bash
python scripts/spark_streaming_to_postgres.py
```

Wait for `Waiting for incoming events...`. Spark monitors the incoming directory and processes new CSV files.

### Terminal 2: Start the Event Generator

In a second terminal (same environment, project root) run:

```bash
python scripts/data_generator.py
```

The generator creates 10 simulated e-commerce events every 5 seconds and saves them as CSV files in `data/incoming/`.

Optional flags: `--events N` (events per file), `--interval SECONDS` (pause between files) and `--files N` (stop after N files). For example, a heavier load test:

```bash
python scripts/data_generator.py --events 500 --interval 1 --files 40
```

## 8. Monitoring

Open the Spark Web UI:

```text
http://localhost:4040
```

Use the interface to inspect Spark jobs, stages, and streaming execution.

Performance metrics are recorded in:

```text
reports/streaming_metrics.csv
```

Each row has the batch ID, records processed, records inserted, records rejected as invalid and the batch duration. A log line is also printed for every batch, and a separate line when records are rejected.

To summarise throughput and latency:

```bash
python scripts/measure_performance.py
```

## 9. Verifying Database Records

Connect to PostgreSQL and execute:

```sql
SELECT COUNT(*) FROM user_events;

SELECT *
FROM user_events
ORDER BY ingestion_time DESC
LIMIT 10;
```

These queries help verify that events are being inserted successfully.

## 10. Running the Tests

```bash
python -m pytest tests/ -v
```

The database tests need PostgreSQL running with the table populated. The generator and transformation tests need only Java and PySpark. See `docs/test_cases.md`.

## 11. Stopping the Pipeline

- Press `Ctrl + C` in the Spark terminal to stop Spark.
- Press `Ctrl + C` in the event generator terminal to stop event generation.

## 12. Troubleshooting

### Hadoop Configuration Error

If Spark reports that `HADOOP_HOME` is unset, verify that:

```text
D:\hadoop\bin\winutils.exe
```

exists and that the Hadoop environment variables are configured.

### PostgreSQL Connection Error

Check that PostgreSQL is running and that the `.env` database settings are correct.

### No Incoming Events

Ensure the event generator is running and creating CSV files in `data/incoming/`.

### Records Are Rejected

Rejected records are invalid rows (missing fields, unsupported event type, price of zero or less, malformed timestamp). The Spark log shows `rejected N invalid record(s)` for the batch. Inspect the CSV file to find them.

### Spark Does Not Start

Verify the Python environment, PySpark installation, Java configuration, and project working directory.

## 13. Conclusion

Following these instructions allows developers to configure, run, monitor, and verify the Real-Time Data Ingestion Pipeline.