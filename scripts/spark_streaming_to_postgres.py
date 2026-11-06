
import os
from pathlib import Path

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


# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_DIR / ".env")

INPUT_DIR = PROJECT_DIR / "data" / "incoming"
CHECKPOINT_DIR = PROJECT_DIR / "checkpoint"

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
# 6. Write each micro-batch to PostgreSQL
# --------------------------------------------------

def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No valid events.")
        return

    rows = batch_df.select(
        "event_id",
        "user_id",
        "product_id",
        "event_type",
        "price",
        "event_time",
    ).collect()

    import psycopg2
    from psycopg2.extras import execute_values

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

                execute_values(
                    cursor,
                    insert_query,
                    values,
                    page_size=1000,
                )

        print(
            f"Batch {batch_id}: "
            f"{len(rows)} events processed."
        )

    finally:
        connection.close()


# --------------------------------------------------
# 7. Start streaming query
# --------------------------------------------------

query = (
    processed_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark Structured Streaming started.")
print("Waiting for incoming CSV files...")

query.awaitTermination()
