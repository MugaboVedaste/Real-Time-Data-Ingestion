# User Guide – Real-Time Data Ingestion Pipeline

## 1. Introduction

This guide explains how to install, configure, and run the Real-Time Data Ingestion Pipeline using Python, Apache Spark Structured Streaming, and PostgreSQL.

## 2. Prerequisites

Before running the project, ensure you have installed:

- Python 3.11 or compatible version
- Java Development Kit (JDK)
- Apache Spark / PySpark
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
2. Create a database named `ecommerce_streaming`.
3. Execute the SQL script in the project's `sql/` directory to create the required table.
4. Configure the database connection variables in the `.env` file.

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

Ensure `winutils.exe` is available at:

```text
D:\hadoop\bin\winutils.exe
```

The Python streaming script configures the Hadoop environment variables before creating the Spark session.

## 7. Running the Pipeline

### Terminal 1: Start the Event Generator

Run the event generator script:

```bash
python scripts/data_generator.py
```

The generator continuously creates simulated e-commerce events and saves them as CSV files in `data/incoming/`.

### Terminal 2: Start Spark Streaming

Activate the configured Python environment and navigate to the project directory.

Run:

```bash
python scripts/spark_streaming_to_postgres.py
```

Spark will monitor the incoming directory and process new CSV files.

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

## 10. Stopping the Pipeline

- Press `Ctrl + C` in the Spark terminal to stop Spark.
- Press `Ctrl + C` in the event generator terminal to stop event generation.

## 11. Troubleshooting

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

### Spark Does Not Start

Verify the Python environment, PySpark installation, Java configuration, and project working directory.

## 12. Conclusion

Following these instructions allows developers to configure, run, monitor, and verify the Real-Time Data Ingestion Pipeline.