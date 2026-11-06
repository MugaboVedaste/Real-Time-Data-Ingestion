
import argparse
import csv
import os
import random
import time

from datetime import datetime
from pathlib import Path


# Project directories
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = Path(os.getenv("INPUT_DIR", PROJECT_DIR / "data" / "incoming"))

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# E-commerce event configuration
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

PRODUCTS = [
    {"product_id": 501, "price": 120.00},
    {"product_id": 502, "price": 250.00},
    {"product_id": 503, "price": 85.50},
    {"product_id": 504, "price": 450.00},
    {"product_id": 505, "price": 75.00},
]

DEFAULT_NUM_EVENTS = 10
DEFAULT_INTERVAL_SECONDS = 5


def generate_event(event_id):
    """Generate one fake e-commerce event."""

    product = random.choice(PRODUCTS)

    event = {
        "event_id": event_id,
        "user_id": random.randint(1001, 1100),
        "product_id": product["product_id"],
        "event_type": random.choice(EVENT_TYPES),
        "price": product["price"],
        "event_time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }

    return event


def generate_batch(num_events=DEFAULT_NUM_EVENTS):
    """Generate a batch of events and save them to a CSV file."""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    file_path = OUTPUT_DIR / f"events_{timestamp}.csv"

    # Write to a hidden temp file first (Spark ignores names starting with
    # "."), then rename. Spark can then never read a half-written file.
    temp_path = OUTPUT_DIR / f".events_{timestamp}.csv.tmp"

    # Generate unique IDs using the current nanosecond timestamp
    base_event_id = time.time_ns()

    events = [
        generate_event(base_event_id + i)
        for i in range(num_events)
    ]

    fieldnames = [
        "event_id",
        "user_id",
        "product_id",
        "event_type",
        "price",
        "event_time",
    ]

    with open(temp_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(events)

    os.replace(temp_path, file_path)

    print(
        f"Generated {num_events} events -> {file_path.name}"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Continuously generate fake e-commerce event CSV files."
    )
    parser.add_argument(
        "--events", type=int, default=DEFAULT_NUM_EVENTS,
        help="events per CSV file (default: %(default)s)",
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL_SECONDS,
        help="seconds between files (default: %(default)s)",
    )
    parser.add_argument(
        "--files", type=int, default=0,
        help="stop after this many files (default: run until CTRL+C)",
    )
    return parser.parse_args()


def main():
    """Continuously generate CSV event batches."""

    args = parse_args()

    print("Starting e-commerce data generator...")
    print(f"Output directory: {OUTPUT_DIR}")
    print("Press CTRL+C to stop.")

    try:
        generated = 0

        while args.files == 0 or generated < args.files:
            generate_batch(args.events)
            generated += 1
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nData generation stopped.")


if __name__ == "__main__":
    main()
