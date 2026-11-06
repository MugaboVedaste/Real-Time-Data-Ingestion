
import os
import csv
import time
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DoubleType,
)

os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["hadoop.home.dir"] = r"D:\hadoop"
# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_DIR / ".env")

INPUT_DIR = PROJECT_DIR / "data" / "incoming"
CHECKPOINT_DIR = PROJECT_DIR / "checkpoint"
REPORTS_DIR = PROJECT_DIR / "reports"

# Create required directories
INPUT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Streaming metrics output
METRICS_FILE = REPORTS_DIR / "streaming_metrics.csv"

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce_streaming")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise ValueError("POSTGRES_PASSWORD is missing in .env")


# --------------------------------------------------
# 2. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("EcommerceRealTimeIngestion")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 3. Define CSV schema
# --------------------------------------------------

event_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True),
])


# --------------------------------------------------
# 4. Read streaming CSV files
# --------------------------------------------------

streaming_df = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .schema(event_schema)
    .load(str(INPUT_DIR))
)


# --------------------------------------------------
# 5. Transform and clean data
# --------------------------------------------------

processed_df = (
    streaming_df
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
    )
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("event_type").isin("view", "add_to_cart", "purchase"))
    .filter(col("price").isNotNull() & (col("price") > 0))
    .filter(col("event_time").isNotNull())
)


# --------------------------------------------------
# 6. Write each micro-batch to PostgreSQL with metrics
# --------------------------------------------------

def write_to_postgres(batch_df, batch_id):

    start_time = time.perf_counter()
    processed_count = 0
    inserted_count = 0

    import psycopg2
    from psycopg2.extras import execute_values

    try:
        if not batch_df.isEmpty():

            rows = batch_df.select(
                "event_id",
                "user_id",
                "product_id",
                "event_type",
                "price",
                "event_time",
            ).collect()

            processed_count = len(rows)

            if rows:

                connection = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    database=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                )

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

        processing_timestamp = datetime.now(timezone.utc).isoformat()

        # Save metrics to CSV
        file_exists = METRICS_FILE.exists()

        with open(
            METRICS_FILE,
            "a",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.writer(file)

            if not file_exists:
                writer.writerow([
                    "processing_timestamp",
                    "batch_id",
                    "processed_records",
                    "inserted_records",
                    "duration_seconds",
                ])

            writer.writerow([
                processing_timestamp,
                batch_id,
                processed_count,
                inserted_count,
                duration,
            ])

        # Display metrics
        print(
            f"\nBatch {batch_id} Metrics | "
            f"Processed: {processed_count} | "
            f"Inserted: {inserted_count} | "
            f"Duration: {duration}s",
            flush=True
        )

    except Exception as error:
        print(f"Batch {batch_id} failed: {error}", flush=True)
        raise


# --------------------------------------------------
# 7. Start Spark Structured Streaming
# --------------------------------------------------

try:

    query = (
        processed_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(
        "\nSpark Structured Streaming started successfully!",
        flush=True
    )

    print(
        "Waiting for incoming events...",
        flush=True
    )

    print(
        f"Spark UI: http://localhost:{spark.sparkContext.uiWebUrl.split(':')[-1]}"
        if spark.sparkContext.uiWebUrl
        else "Spark UI is not available.",
        flush=True
    )

    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStreaming application interrupted by user.")

finally:
    spark.stop()
