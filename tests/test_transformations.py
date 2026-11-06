"""Tests for the Spark cleaning logic, run on a small local batch DataFrame.

These need Java and PySpark but not PostgreSQL or a running stream.
"""

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import spark_streaming_to_postgres as pipeline  # noqa: E402

GOOD = "2026-11-06 10:00:00"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    pipeline.configure_hadoop_for_windows()

    session = (
        SparkSession.builder
        .appName("pipeline-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


def run_cleaning(spark, tmp_path, rows):
    """Write rows to a CSV and read it with the pipeline's own schema.

    Reading a file (like the real stream does) keeps everything on the JVM, so
    no Python worker process is needed.
    """

    path = tmp_path / "batch.csv"
    lines = ["event_id,user_id,product_id,event_type,price,event_time"]
    lines += [",".join("" if v is None else str(v) for v in row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    df = (
        spark.read
        .option("header", "true")
        .schema(pipeline.EVENT_SCHEMA)
        .csv(str(path))
    )

    return pipeline.clean_events(pipeline.parse_events(df)).collect()


def test_valid_row_is_kept_and_types_converted(spark, tmp_path):
    rows = run_cleaning(spark, tmp_path, [(1, 1001, 501, "view", "120.00", GOOD)])

    assert len(rows) == 1
    assert rows[0]["event_time"] == datetime(2026, 11, 6, 10, 0, 0)
    assert rows[0]["price"] == Decimal("120.00")


def test_all_supported_event_types_are_kept(spark, tmp_path):
    rows = run_cleaning(spark, tmp_path, [
        (1, 1001, 501, "view", 10, GOOD),
        (2, 1001, 501, "add_to_cart", 10, GOOD),
        (3, 1001, 501, "purchase", 10, GOOD),
    ])

    assert {r["event_type"] for r in rows} == {"view", "add_to_cart", "purchase"}


@pytest.mark.parametrize(
    "bad_row",
    [
        (None, 1001, 501, "view", 10, GOOD),         # missing event_id
        (1, None, 501, "view", 10, GOOD),            # missing user_id
        (1, 1001, None, "view", 10, GOOD),           # missing product_id
        (1, 1001, 501, "refund", 10, GOOD),          # unsupported type
        (1, 1001, 501, None, 10, GOOD),              # missing type
        (1, 1001, 501, "view", None, GOOD),          # missing price
        (1, 1001, 501, "view", 0, GOOD),             # zero price
        (1, 1001, 501, "view", -5, GOOD),            # negative price
        (1, 1001, 501, "view", "abc", GOOD),         # non-numeric price
        (1, 1001, 501, "view", 10, "not-a-date"),    # bad timestamp
        (1, 1001, 501, "view", 10, None),            # missing timestamp
    ],
)
def test_invalid_rows_are_rejected(spark, tmp_path, bad_row):
    assert run_cleaning(spark, tmp_path, [bad_row]) == []


def test_mixed_batch_keeps_only_valid_rows(spark, tmp_path):
    rows = run_cleaning(spark, tmp_path, [
        (1, 1001, 501, "view", 10, GOOD),
        (2, 1001, 501, "refund", 10, GOOD),
        (3, 1001, 501, "purchase", -1, GOOD),
        (4, 1002, 502, "purchase", 250, GOOD),
    ])

    assert sorted(r["event_id"] for r in rows) == [1, 4]
