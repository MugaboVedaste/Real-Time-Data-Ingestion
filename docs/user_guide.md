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
git clone https://github.com/MugaboVedaste/Real-Time-Data-Ingestion.git
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

You can start everything with one command (Option A) or run each script yourself (Option B).

### Option A: Run Everything With One Command

Activate the Python environment, go to the project root and run:

```bash
python scripts/run_pipeline.py
```

This starts Spark, waits until it is ready, then starts the event generator. Press `Ctrl + C` once to stop both; a performance summary for the session is then printed.

Options:

| Flag | Meaning |
|---|---|
| `--events N` | Events per CSV file (default 10) |
| `--interval SECONDS` | Seconds between files (default 5) |
| `--files N` | Generate N files, let Spark finish, then exit (default: run until `Ctrl + C`) |
| `--no-measure` | Skip the performance summary |

Example (a heavier test that stops by itself):

```bash
python scripts/run_pipeline.py --events 500 --interval 1 --files 40
```

The launcher only starts the existing scripts, so Option B still works exactly as before.

### Option B: Run Each Script Manually

#### Terminal 1: Start Spark Streaming

Activate the configured Python environment, navigate to the project directory and run:

```bash
python scripts/spark_streaming_to_postgres.py
```

Wait for `Waiting for incoming events...`. Spark monitors the incoming directory and processes new CSV files.

#### Terminal 2: Start the Event Generator

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

- If you used `run_pipeline.py`, press `Ctrl + C` once in its terminal. It stops both processes.
- If you started the scripts manually, press `Ctrl + C` in the event generator terminal, then in the Spark terminal.

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