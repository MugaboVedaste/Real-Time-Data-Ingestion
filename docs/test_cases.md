# Test Cases and Results – Real-Time Data Ingestion Pipeline

## 1. Introduction

This document describes the tests performed to verify the reliability and correctness of the Real-Time Data Ingestion Pipeline.

The tests focus on PostgreSQL connectivity, event insertion, duplicate prevention, data validation, and required fields.

## 2. Test Environment

| Component | Configuration |
|---|---|
| Programming Language | Python |
| Streaming Framework | Apache Spark Structured Streaming |
| Database | PostgreSQL |
| Test Framework | pytest |
| Database | ecommerce_streaming |
| Main Table | user_events |

## 3. Test Cases

| Test ID | Test Description | Expected Result |
|---|---|---|
| TC-01 | Test PostgreSQL database connection | Database connection succeeds |
| TC-02 | Verify event records in the database | Event records are retrieved successfully |
| TC-03 | Check duplicate event IDs | Duplicate records are prevented |
| TC-04 | Validate event types | Only view, add_to_cart, and purchase are accepted |
| TC-05 | Validate event prices | Prices must be greater than zero |
| TC-06 | Validate required fields | Required fields cannot be NULL |

## 4. Test Execution Results

The automated test suite was executed using pytest.

Command:

```bash
pytest -v
```

Result:

**6 tests passed successfully.**

| Test ID | Result |
|---|---|
| TC-01 | PASSED |
| TC-02 | PASSED |
| TC-03 | PASSED |
| TC-04 | PASSED |
| TC-05 | PASSED |
| TC-06 | PASSED |

## 5. Streaming Pipeline Validation

In addition to automated tests, the running pipeline was checked using PostgreSQL queries.

### Verify total records

```sql
SELECT COUNT(*) AS total_events
FROM user_events;
```

### Check duplicate event IDs

```sql
SELECT event_id, COUNT(*)
FROM user_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

### Check event types

```sql
SELECT event_type, COUNT(*)
FROM user_events
GROUP BY event_type;
```

These checks help verify that records are stored correctly and duplicate event IDs are not present.

## 6. Conclusion

The automated test suite completed successfully with six passing tests.

The tests verified database connectivity, event retrieval, duplicate prevention, valid event types, positive prices, and required field constraints.

The streaming pipeline was also validated through database queries and recorded processing metrics.