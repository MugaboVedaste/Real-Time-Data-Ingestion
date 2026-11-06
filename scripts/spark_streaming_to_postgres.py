import os
import csv
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, try_to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DecimalType,
)


# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_DIR / ".env")

# Paths default to the project folders; override them with env vars to run an
# isolated session (for example a performance test) without touching real data.
INPUT_DIR = Path(os.getenv("INPUT_DIR", PROJECT_DIR / "data" / "incoming"))
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", PROJECT_DIR / "checkpoint"))
METRICS_FILE = Path(
    os.getenv("METRICS_FILE", PROJECT_DIR / "reports" / "streaming_metrics.csv")
)
REPORTS_DIR = METRICS_FILE.parent

METRICS_HEADER = [
    "processing_timestamp",
    "batch_id",
    "processed_records",
    "inserted_records",
    "rejected_records",
    "duration_seconds",
]

VALID_EVENT_TYPES = ("view", "add_to_cart", "purchase")

# How many new files Spark picks up per micro-batch. A value above 1 lets the
# pipeline catch up when files arrive faster than one per trigger.
MAX_FILES_PER_TRIGGER = int(os.getenv("MAX_FILES_PER_TRIGGER", "10"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")


def configure_hadoop_for_windows():
    """Spark on Windows needs winutils.exe. Skip everywhere else.

    The location is read from HADOOP_HOME (set it in .env or your shell) and
    falls back to D:\\hadoop, which is where the original development machine
    keeps it.
    """

    if not sys.platform.startswith("win"):
        return

    hadoop_home = os.getenv("HADOOP_HOME") or r"D:\hadoop"

    if not (Path(hadoop_home) / "bin" / "winutils.exe").exists():
        print(
            f"WARNING: winutils.exe not found in {hadoop_home}\\bin. "
            "Set HADOOP_HOME to the folder that contains bin\\winutils.exe.",
            flush=True,
        )

    os.environ["HADOOP_HOME"] = hadoop_home


# --------------------------------------------------
# 2. CSV schema
# --------------------------------------------------

EVENT_SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("event_time", StringType(), True),
])


# --------------------------------------------------
# 3. Transformations (pure DataFrame functions, unit-testable)
# --------------------------------------------------

def parse_events(df):
    """Convert event_time from text to a real timestamp.

    try_to_timestamp returns NULL for malformed values (Spark 4 runs in ANSI
    mode, where to_timestamp would raise and crash the stream). The NULL rows
    are then rejected by clean_events.
    """

    return df.withColumn(
        "event_time",
        try_to_timestamp(col("event_time"), lit("yyyy-MM-dd HH:mm:ss")),
    )


def clean_events(df):
    """Keep only valid rows from a parsed events DataFrame."""

    return (
        df
        .filter(col("event_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("event_type").isin(*VALID_EVENT_TYPES))
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("event_time").isNotNull())
    )


# --------------------------------------------------
# 4. Metrics helper
# --------------------------------------------------

def append_metrics(row):
    """Append one metrics row, upgrading an older file header if needed."""

    if METRICS_FILE.exists():
        with open(METRICS_FILE, newline="", encoding="utf-8") as file:
            existing = list(csv.reader(file))

        if existing and existing[0] != METRICS_HEADER:
            # Older files have no rejected_records column. Keep the old rows
            # and leave that value blank, since it was never measured.
            old_header = existing[0]
            upgraded = [METRICS_HEADER]
            for old_row in existing[1:]:
                record = dict(zip(old_header, old_row))
                upgraded.append([record.get(name, "") for name in METRICS_HEADER])

            with open(METRICS_FILE, "w", newline="", encoding="utf-8") as file:
                csv.writer(file).writerows(upgraded)

    write_header = not METRICS_FILE.exists()

    with open(METRICS_FILE, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        if write_header:
            writer.writerow(METRICS_HEADER)

        writer.writerow(row)


# --------------------------------------------------
# 5. Write each micro-batch to PostgreSQL
# --------------------------------------------------

def make_batch_writer(db_config):

    def write_to_postgres(batch_df, batch_id):

        import psycopg2
        from psycopg2.extras import execute_values

        start_time = time.perf_counter()
        processed_count = 0
        inserted_count = 0
        rejected_count = 0

        try:
            # The batch is read twice (total count, then the valid rows).
            batch_df = batch_df.cache()

            try:
                total_count = batch_df.count()

                rows = clean_events(batch_df).select(
                    "event_id",
                    "user_id",
                    "product_id",
                    "event_type",
                    "price",
                    "event_time",
                ).collect()
            finally:
                batch_df.unpersist()

            processed_count = len(rows)
            rejected_count = total_count - processed_count

            if rejected_count:
                print(
                    f"Batch {batch_id}: rejected {rejected_count} invalid "
                    f"record(s) out of {total_count}.",
                    flush=True,
                )

            if rows:
                connection = psycopg2.connect(**db_config)

                try:
                    with connection:
                        with connection.cursor() as cursor:

                            insert_query = """
                                INSERT INTO user_events (
                                    event_id,
                                    user_id,
                                    product_id,
                                    event_type,
                                    price,
                                    event_time
                                )
                                VALUES %s
                                ON CONFLICT (event_id) DO NOTHING
                                RETURNING event_id
                            """

                            values = [
                                (
                                    row["event_id"],
                                    row["user_id"],
                                    row["product_id"],
                                    row["event_type"],
                                    row["price"],
                                    row["event_time"],
                                )
                                for row in rows
                            ]

                            inserted_rows = execute_values(
                                cursor,
                                insert_query,
                                values,
                                page_size=1000,
                                fetch=True,
                            )

                            inserted_count = len(inserted_rows)

                finally:
                    connection.close()

            duration = round(time.perf_counter() - start_time, 3)

            append_metrics([
                datetime.now(timezone.utc).isoformat(),
                batch_id,
                processed_count,
                inserted_count,
                rejected_count,
                duration,
            ])

            print(
                f"\nBatch {batch_id} Metrics | "
                f"Processed: {processed_count} | "
                f"Inserted: {inserted_count} | "
                f"Rejected: {rejected_count} | "
                f"Duration: {duration}s",
                flush=True,
            )

        except Exception as error:
            print(f"Batch {batch_id} failed: {error}", flush=True)
            raise

    return write_to_postgres


# --------------------------------------------------
# 6. Main
# --------------------------------------------------

def main():

    configure_hadoop_for_windows()

    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise ValueError("POSTGRES_PASSWORD is missing in .env")

    db_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "ecommerce_streaming"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": password,
    }

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("EcommerceRealTimeIngestion")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    streaming_df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(EVENT_SCHEMA)
        .load(str(INPUT_DIR))
    )

    # Only type conversion happens on the stream. Validation runs inside
    # foreachBatch so rejected records can be counted instead of vanishing.
    parsed_df = parse_events(streaming_df)

    try:
        query = (
            parsed_df.writeStream
            .foreachBatch(make_batch_writer(db_config))
            .outputMode("append")
            .option("checkpointLocation", str(CHECKPOINT_DIR))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )

        print("\nSpark Structured Streaming started successfully!", flush=True)
        print("Waiting for incoming events...", flush=True)
        print(
            f"Spark UI: {spark.sparkContext.uiWebUrl}"
            if spark.sparkContext.uiWebUrl
            else "Spark UI is not available.",
            flush=True,
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        print("\nStreaming application interrupted by user.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
