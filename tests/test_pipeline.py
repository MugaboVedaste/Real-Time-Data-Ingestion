
import os
import psycopg2
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def db_connection():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "ecommerce_streaming"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    yield connection
    connection.close()


def test_database_connection(db_connection):
    assert db_connection.closed == 0


def test_events_exist(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM user_events")
        count = cursor.fetchone()[0]

    assert count > 0


def test_no_duplicate_event_ids(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute("""
            SELECT event_id
            FROM user_events
            GROUP BY event_id
            HAVING COUNT(*) > 1
        """)

        duplicates = cursor.fetchall()

    assert len(duplicates) == 0


def test_valid_event_types(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_events
            WHERE event_type NOT IN (
                'view',
                'add_to_cart',
                'purchase'
            )
        """)

        invalid_count = cursor.fetchone()[0]

    assert invalid_count == 0


def test_positive_prices(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_events
            WHERE price <= 0
        """)

        invalid_count = cursor.fetchone()[0]

    assert invalid_count == 0


def test_required_fields_not_null(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_events
            WHERE event_id IS NULL
               OR user_id IS NULL
               OR product_id IS NULL
               OR event_type IS NULL
               OR price IS NULL
               OR event_time IS NULL
        """)

        invalid_count = cursor.fetchone()[0]

    assert invalid_count == 0
