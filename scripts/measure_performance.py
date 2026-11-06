"""Summarise pipeline performance from the metrics CSV and the database.

Usage:
    python scripts/measure_performance.py
    python scripts/measure_performance.py --metrics path/to/metrics.csv \
        --database my_db --since "2026-11-06 12:00:00"

Reports:
  * batch duration (avg / min / max / p95)
  * processing capacity: records per second of Spark busy time
  * end-to-end throughput: records per second of wall-clock time
  * end-to-end latency: ingestion_time - event_time, from PostgreSQL
"""

import argparse
import csv
import os
import statistics
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_DIR / ".env")


def percentile(values, pct):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round(pct / 100 * len(ordered)) - 1))
    return ordered[index]


def load_metrics(path, since):
    rows = []

    with open(path, newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            stamp = datetime.fromisoformat(row["processing_timestamp"])
            # The file stores UTC; --since is given in local time.
            if since and stamp.astimezone().replace(tzinfo=None) < since:
                continue
            rows.append({
                "timestamp": stamp,
                "processed": int(row["processed_records"] or 0),
                "inserted": int(row["inserted_records"] or 0),
                "rejected": row.get("rejected_records", ""),
                "duration": float(row["duration_seconds"]),
            })

    return rows


def latency_stats(database, since):
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=database or os.getenv("POSTGRES_DB", "ecommerce_streaming"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*),
                       AVG(EXTRACT(EPOCH FROM ingestion_time - event_time)),
                       MIN(EXTRACT(EPOCH FROM ingestion_time - event_time)),
                       MAX(EXTRACT(EPOCH FROM ingestion_time - event_time)),
                       PERCENTILE_CONT(0.95) WITHIN GROUP (
                           ORDER BY EXTRACT(EPOCH FROM ingestion_time - event_time)
                       )
                FROM user_events
                WHERE (%s::timestamp IS NULL OR event_time >= %s::timestamp)
                """,
                (since, since),
            )
            return cursor.fetchone()
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--metrics",
        default=os.getenv(
            "METRICS_FILE", str(PROJECT_DIR / "reports" / "streaming_metrics.csv")
        ),
    )
    parser.add_argument("--database", default=None)
    parser.add_argument(
        "--since", default=None,
        help="only include data at or after this local time (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--no-db", action="store_true", help="skip the latency query"
    )
    args = parser.parse_args()

    since = datetime.fromisoformat(args.since) if args.since else None
    rows = load_metrics(args.metrics, since)

    if not rows:
        raise SystemExit("No metrics rows found.")

    durations = [r["duration"] for r in rows]
    processed = sum(r["processed"] for r in rows)
    inserted = sum(r["inserted"] for r in rows)
    busy = sum(durations)
    wall = (rows[-1]["timestamp"] - rows[0]["timestamp"]).total_seconds()
    rejected_known = [r["rejected"] for r in rows if r["rejected"] != ""]
    rejected = sum(int(v) for v in rejected_known) if rejected_known else "n/a"

    print(f"batches                 : {len(rows)}")
    print(f"records processed       : {processed}")
    print(f"records inserted        : {inserted}")
    print(f"records rejected        : {rejected}")
    print(f"batch duration avg (s)  : {statistics.mean(durations):.3f}")
    print(f"batch duration min (s)  : {min(durations):.3f}")
    print(f"batch duration max (s)  : {max(durations):.3f}")
    print(f"batch duration p95 (s)  : {percentile(durations, 95):.3f}")
    print(f"processing capacity     : {processed / busy:.1f} records/s of busy time")

    if wall > 0:
        # First and last batch timestamps bound (n-1) intervals, so count the
        # records that finished after the first batch.
        after_first = processed - rows[0]["processed"]
        print(f"end-to-end throughput   : {after_first / wall:.1f} records/s "
              f"over {wall:.0f}s wall-clock")

    if not args.no_db:
        count, avg, low, high, p95 = latency_stats(args.database, since)
        if count:
            print(f"latency rows            : {count}")
            print(f"latency avg (s)         : {avg:.2f}")
            print(f"latency min (s)         : {low:.2f}")
            print(f"latency p95 (s)         : {p95:.2f}")
            print(f"latency max (s)         : {high:.2f}")
            print("(event_time has 1-second resolution, so latency is +/- 1s)")


if __name__ == "__main__":
    main()
