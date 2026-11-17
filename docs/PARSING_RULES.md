# Common Parsing Rules
## DAS 839 - NoSQL Systems End Semester Project

## 1. Raw Input Format

Each input record follows the NASA HTTP log format:

host ident authuser [timestamp] "request" status bytes

Example:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

The ident and authuser fields are not required in the
structured output.

## 2. Required Structured Fields

| Field | Type |
|---|---|
| host | STRING |
| timestamp | STRING |
| log_date | DATE |
| log_hour | INTEGER |
| http_method | STRING / NULL |
| resource_path | STRING / NULL |
| protocol_version | STRING / NULL |
| status_code | INTEGER |
| bytes_transferred | BIGINT |

## 3. Host Extraction

- Extract the first field.
- Preserve the original value.
- Do not normalize case or perform DNS resolution.
- Missing host or host represented by '-' is malformed.

## 4. Timestamp Extraction

Expected format:

DD/Mon/YYYY:HH:MM:SS +/-HHMM

Example:

01/Jul/1995:00:00:01 -0400

Rules:

- Preserve the original timestamp.
- Parse using the corresponding date-time format.
- Derive log_date in YYYY-MM-DD format.
- Derive log_hour as an integer from 0 to 23.
- Preserve the original logged clock time.
- Do not convert timestamps to UTC or machine-local time.

Invalid or missing timestamps make the record malformed.

## 5. HTTP Request Parsing

The request is enclosed in double quotes.

Example:

"GET /history/apollo/ HTTP/1.0"

Split the request into:

- First component: http_method
- Second component: resource_path
- Third component: protocol_version

Rules:

- Preserve the original method and its case.
- Preserve the resource path exactly.
- Preserve query parameters.
- Do not URL-decode or normalize resource paths.
- Do not remove query parameters.
- A request represented by "-" is valid, with all
  three request components set to NULL.
- If only one or two components are present, extract
  the available components and set missing components
  to NULL.
- A non-empty request with more than three
  whitespace-separated components is malformed.
- An unclosed or structurally invalid quoted request
  makes the record malformed.

## 6. Status Code

- Extract the response status.
- Store it as an integer.
- Require a valid three-digit numeric status.
- Missing or invalid status makes the record malformed.

## 7. Bytes Transferred

- Numeric values are converted to BIGINT.
- A value of '-' is converted to 0.
- A missing bytes field is converted to 0.
- A non-numeric value other than '-' is malformed.
- Negative values are invalid.

## 8. Malformed Record Handling

A record is malformed if:

1. The outer log structure cannot be parsed.
2. The host is missing or represented by '-'.
3. The timestamp is missing or invalid.
4. The request field is structurally invalid.
5. The request has more than three components.
6. The status code is missing or invalid.
7. The bytes field is non-numeric and not '-'.

Each malformed input line must be counted exactly once.

Malformed records must not silently disappear.
Maintain per-batch and per-run malformed counts.

## 9. Query Eligibility

### Q1: Daily Traffic Summary

Include every valid parsed record.

Group by log_date and status_code.

Calculate:
- Request count
- Total bytes transferred

### Q2: Top Requested Resources

Include valid records with a non-NULL resource_path.

Group by resource_path.

Calculate:
- Request count
- Total bytes transferred
- Distinct host count

Return the global top 20 resources.

For equal request counts, sort resource_path
in ascending order as a deterministic tie-breaker.

### Q3: Hourly Error Analysis

Include every valid parsed record.

Group by log_date and log_hour.

Calculate:
- Error request count (status 400-599)
- Total request count
- Error rate
- Distinct hosts generating error requests

Error rate:

error request count / total request count

Represent error rate as a fraction between 0 and 1.

## 10. Batching Consistency

- Batch size means original input records.
- Batch IDs begin at 1.
- Every non-empty batch is counted.
- The final partial batch is valid.
- Malformed records are counted in their respective batches.
- Global aggregations must combine information
  across all batches correctly.
- Distinct-host counts must not be added directly
  across batches.

## 11. Cross-Pipeline Equivalence

Pig, MapReduce, MongoDB and Hive must implement
the same rules.

No pipeline may independently change:

- Parsing behaviour
- Missing-value handling
- Malformed-record criteria
- Query eligibility
- Grouping keys
- Aggregation semantics
- Output schemas

The same test cases must be used to validate
all four implementations.
