# Common Data Model - NASA HTTP Log Analytics

## 1. Dataset

Official NASA HTTP Web Server Logs:
- NASA_access_log_Jul95
- NASA_access_log_Aug95

Both original files are retained in data/raw/.

## 2. Common Structured Schema

| Field | Logical Type | Description |
|---|---|---|
| host | STRING | Requesting host |
| timestamp | STRING | Original logged timestamp |
| log_date | DATE | Date extracted from timestamp |
| log_hour | INTEGER | Hour extracted from timestamp |
| http_method | STRING | HTTP request method |
| resource_path | STRING | Requested resource |
| protocol_version | STRING | HTTP protocol version |
| status_code | INTEGER | HTTP response status |
| bytes_transferred | BIGINT | Bytes returned |

## 3. Processing Metadata

- source_file
- record_number
- batch_id

## 4. Parsing Rules

1. Parse the original NASA log format.
2. Extract host, timestamp, request, status and bytes.
3. Extract method, resource path and protocol where present.
4. Derive log_date and log_hour from the original logged clock time.
5. Do not convert timestamps to the machine's local timezone.
6. Treat '-' or missing bytes as 0.
7. Preserve unavailable request components as NULL when the
   request itself is '-'.
8. Count records that cannot be parsed as malformed.
9. Do not silently discard malformed records.
10. Do not manually preprocess the raw dataset outside
    the selected execution pipeline.

## 5. Query Definitions

### Q1: Daily Traffic Summary
Group by log_date and status_code.
Calculate request count and total bytes.

### Q2: Top Requested Resources
Group by resource_path.
Calculate request count, total bytes and distinct host count.
Return the top 20 resources by request count.

### Q3: Hourly Error Analysis
Group by log_date and log_hour.
Calculate error request count (status 400-599),
total request count, error rate and distinct error hosts.

## 6. Equivalence Requirement

All four pipelines must use the same input records,
parsing rules, cleaning rules, query definitions,
aggregation semantics and output schemas.
