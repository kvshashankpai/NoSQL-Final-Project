# Common Data Model

## DAS 839 - NoSQL Systems End Semester Project

## 1. Purpose

This document defines the common logical data model shared by all four ETL pipelines:

1. Apache Pig
2. Hadoop MapReduce
3. MongoDB
4. Apache Hive

Each pipeline may use its own internal data structures and storage formats. However, all pipelines must produce equivalent logical records and JSONL outputs.

## 2. Source Dataset

Dataset: NASA HTTP Web Server Logs.

Input files, in processing order:

1. `data/raw/NASA_access_log_Jul95`
2. `data/raw/NASA_access_log_Aug95`

Input encoding: ISO-8859-1.

One physical line represents one input record. Record numbering is continuous across both files.

## 3. Parsed Log Record

Every valid parsed record follows this logical schema:

| Field | Logical type | Nullable | Description |
|---|---|---|---|
| host | STRING | No | Original requesting host |
| timestamp | STRING | No | Original logged timestamp |
| log_date | DATE | No | Date extracted from timestamp |
| log_hour | INTEGER | No | Logged hour, 0-23 |
| http_method | STRING | Yes | First request token |
| resource_path | STRING | Yes | Second request token |
| protocol_version | STRING | Yes | Third request token |
| status_code | INTEGER | No | Three-digit HTTP status |
| bytes_transferred | BIGINT | No | Non-negative response bytes |

The parser must preserve the original host, timestamp, method, resource path and protocol values. No DNS lookup, case normalization, URL decoding or query-string removal is permitted.

Missing request components are represented as NULL.

A missing bytes field or bytes value of `-` is represented as zero.

## 4. Processing Metadata

Each input record is associated with:

| Field | Logical type | Description |
|---|---|---|
| source_file | STRING | Original input filename |
| record_number | BIGINT | Continuous input record number starting at 1 |
| batch_id | INTEGER | Batch identifier starting at 1 |

The record number continues from the July file into the August file.

Batch assignment:

`batch_id = floor((record_number - 1) / batch_size) + 1`

Malformed records also retain their source file, record number and batch ID.

## 5. Batch Metadata

Output file: `batches.jsonl`

One row represents one non-empty batch.

| Field | Logical type | Description |
|---|---|---|
| batch_id | INTEGER | Batch identifier |
| record_count | BIGINT | Actual number of input records |
| malformed_count | BIGINT | Malformed records in this batch |

The configured batch size is a maximum, not the actual count of every batch.

Average batch size:

`total input records / number of non-empty batches`

For empty input, average batch size is NULL.

## 6. Q1 - Daily Traffic Summary

Output file: `daily_traffic.jsonl`

Group valid parsed records by date and status code.

| Field | Logical type | Description |
|---|---|---|
| batch_id | INTEGER / NULL | Positive ID for batch result; NULL for whole-run result |
| log_date | DATE | Request date |
| status_code | INTEGER | HTTP response status |
| request_count | BIGINT | Number of requests |
| total_bytes | BIGINT | Sum of bytes transferred |

All valid records are eligible for Q1, including records whose request components are missing.

## 7. Q2 - Top Requested Resources

Output file: `top_resources.jsonl`

Include valid parsed records with a non-NULL resource path.

Group by the exact resource path.

| Field | Logical type | Description |
|---|---|---|
| batch_id | INTEGER / NULL | Positive ID for batch result; NULL for whole-run result |
| resource_path | STRING | Exact requested resource |
| request_count | BIGINT | Number of requests |
| total_bytes | BIGINT | Sum of bytes transferred |
| distinct_hosts | BIGINT | Number of unique requesting hosts |

Sort by:

1. `request_count` descending.
2. `resource_path` ascending using binary UTF-8 ordering.

Return the top 20 after aggregation.

Whole-run distinct host counts must be computed across the complete dataset. Batch-level distinct counts must not be added together.

## 8. Q3 - Hourly Error Analysis

Output file: `hourly_errors.jsonl`

Group valid parsed records by date and hour.

| Field | Logical type | Description |
|---|---|---|
| batch_id | INTEGER / NULL | Positive ID for batch result; NULL for whole-run result |
| log_date | DATE | Request date |
| log_hour | INTEGER | Logged hour, 0-23 |
| total_requests | BIGINT | All valid requests in the hour |
| error_requests | BIGINT | Requests with status 400-599 inclusive |
| distinct_error_hosts | BIGINT | Unique hosts among error requests |
| error_rate | DECIMAL | Error requests divided by total requests |

Include hours with valid requests even when the error count is zero. Do not synthesize rows for hours with no requests.

Error rate is a decimal fraction between 0 and 1. Cross-pipeline comparisons use an absolute tolerance of `1e-9`.

## 9. Malformed Record Samples

Output file: `malformed_samples.jsonl`

Retain the first 100 malformed records in input order for each run.

| Field | Logical type | Description |
|---|---|---|
| source_file | STRING | Original input filename |
| record_number | BIGINT | Continuous input record number |
| reason | STRING | Malformation reason |
| raw_line | STRING | Original record content after line-terminator removal |

The sample limit applies only to stored examples. All malformed records must contribute to malformed counts.

## 10. JSONL Output Contract

Every adapter produces the following files in its run-specific output directory:

- `batches.jsonl`
- `daily_traffic.jsonl`
- `top_resources.jsonl`
- `hourly_errors.jsonl`
- `malformed_samples.jsonl`

Output encoding is UTF-8.

Each line is one valid JSON object.

Requirements:

- Counts, status codes, hours and byte totals are JSON numbers.
- Missing values are JSON null.
- Dates use `YYYY-MM-DD`.
- Per-batch analytics contain a positive `batch_id`.
- Whole-run analytics contain `"batch_id": null`.
- An empty result is represented by an empty file.
- Top-resource rows follow the required ranking order.
- Row ordering in other result files is not significant.

## 11. Cross-Pipeline Equivalence

All four pipelines must produce equivalent:

- Valid and malformed record classifications.
- Parsed field values.
- Source and record metadata.
- Batch assignments and counts.
- Per-batch query results.
- Whole-run query results.
- Malformed sample ordering and content.
- JSONL field names and logical types.

The common parsing rules and test cases are defined in:

- `PARSING_RULES.md`
- `PARSING_TEST_CASES.md`
- `BATCHING_STRATEGY.md`

These documents define the expected contract. They do not establish that an implementation has already passed validation.
