import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import data_generator  # noqa: E402

EXPECTED_COLUMNS = [
    "event_id",
    "user_id",
    "product_id",
    "event_type",
    "price",
    "event_time",
]


@pytest.fixture
def generated_file(tmp_path, monkeypatch):
    monkeypatch.setattr(data_generator, "OUTPUT_DIR", tmp_path)
    data_generator.generate_batch(num_events=10)

    files = list(tmp_path.glob("events_*.csv"))
    assert len(files) == 1
    return files[0]


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_csv_has_expected_header_and_row_count(generated_file):
    with open(generated_file, newline="", encoding="utf-8") as file:
        header = next(csv.reader(file))

    assert header == EXPECTED_COLUMNS
    assert len(read_rows(generated_file)) == 10


def test_csv_values_are_valid(generated_file):
    valid_prices = {p["price"] for p in data_generator.PRODUCTS}

    for row in read_rows(generated_file):
        assert row["event_type"] in data_generator.EVENT_TYPES
        assert float(row["price"]) in valid_prices
        assert 1001 <= int(row["user_id"]) <= 1100
        assert all(row[column] != "" for column in EXPECTED_COLUMNS)


def test_event_ids_are_unique_across_files(tmp_path, monkeypatch):
    monkeypatch.setattr(data_generator, "OUTPUT_DIR", tmp_path)

    data_generator.generate_batch(num_events=10)
    data_generator.generate_batch(num_events=10)

    ids = [
        row["event_id"]
        for path in tmp_path.glob("events_*.csv")
        for row in read_rows(path)
    ]

    assert len(ids) == 20
    assert len(set(ids)) == 20


def test_no_temp_files_left_behind(generated_file):
    leftovers = list(generated_file.parent.glob(".*.tmp"))
    assert leftovers == []
