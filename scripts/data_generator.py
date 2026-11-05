
import csv
import random
import time

from datetime import datetime
from pathlib import Path


# Project directories
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "data" / "incoming"

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

NUM_EVENTS = 10
INTERVAL_SECONDS = 5


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


def generate_batch():
    """Generate a batch of events and save them to a CSV file."""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    file_path = OUTPUT_DIR / f"events_{timestamp}.csv"

    # Generate unique IDs using the current nanosecond timestamp
    base_event_id = time.time_ns()

    events = [
        generate_event(base_event_id + i)
        for i in range(NUM_EVENTS)
    ]

    fieldnames = [
        "event_id",
        "user_id",
        "product_id",
        "event_type",
        "price",
        "event_time",
    ]

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(events)

    print(
        f"Generated {NUM_EVENTS} events -> {file_path.name}"
    )


def main():
    """Continuously generate CSV event batches."""

    print("Starting e-commerce data generator...")
    print(f"Output directory: {OUTPUT_DIR}")
    print("Press CTRL+C to stop.")

    try:
        while True:
            generate_batch()
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nData generation stopped.")


if __name__ == "__main__":
    main()
