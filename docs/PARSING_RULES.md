# Common Parsing Rules

## DAS 839 - NoSQL Systems End Semester Project

## 1. Raw Input Format

Each input record follows the NASA HTTP log format:

`host ident authuser [timestamp] "request" status bytes`

Example:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

The ident and authuser fields are not required in the structured output.

Input encoding is ISO-8859-1. Each physical line is one input record. The backend adapter removes only the line terminator before parsing.

## 2. Common Structured Schema

| Field | Type | Description |
|---|---|---|
| host | STRING | Requesting host |
| timestamp | STRING | Original logged timestamp |
| log_date | DATE | Date extracted from timestamp |
| log_hour | INTEGER | Hour extracted from timestamp |
| http_method | STRING / NULL | HTTP method |
| resource_path | STRING / NULL | Requested resource |
| protocol_version | STRING / NULL | HTTP protocol |
| status_code | INTEGER | HTTP response status |
| bytes_transferred | BIGINT | Bytes returned |

Processing metadata:

- `source_file`
- `record_number`
- `batch_id`

Record numbering is continuous across the July and August input files.

## 3. Host Extraction

- Extract the first field as host.
- Preserve its original value and case.
- Do not perform DNS resolution or normalization.
- Missing host or host represented by `-` is malformed.

## 4. Timestamp Extraction

Expected timestamp format:

`DD/Mon/YYYY:HH:MM:SS +/-HHMM`

Example:

`01/Jul/1995:00:00:01 -0400`

Rules:

- Preserve the original timestamp string.
- Validate the date and time.
- Use English three-letter month abbreviations.
- Derive `log_date` in `YYYY-MM-DD` format.
- Derive `log_hour` as an integer from 0 to 23.
- Preserve the original logged clock time.
- Do not convert timestamps to UTC or machine-local time.

Invalid or missing timestamps make the record malformed.

## 5. Outer Record Parsing

The parser must identify:

1. Host.
2. Timestamp enclosed in square brackets.
3. Request enclosed in double quotes.
4. Three-digit numeric status code.
5. Optional bytes field.

Horizontal spaces and tabs may separate the outer fields.

The closing request quote must be identified before extracting the status and optional bytes fields.

The bytes field may be absent.

An absent bytes field or a bytes value of `-` is interpreted as zero.

The parser must reject an invalid outer structure rather than silently accepting a partial record.

## 6. HTTP Request Parsing

The request is enclosed in double quotes.

Example:

`"GET /history/apollo/ HTTP/1.0"`

Trim horizontal whitespace from the request before splitting it on horizontal whitespace.

When request tokens exist:

- First token: `http_method`
- Second token: `resource_path`
- Third token: `protocol_version`

Rules:

- Preserve method, resource path and protocol exactly as supplied.
- Preserve host and resource path case.
- Preserve query parameters.
- Do not URL-decode or normalize resource paths.
- Do not remove query parameters.
- A request represented by `-` is valid, with all three request fields set to NULL.
- An empty request is valid, with all three request fields set to NULL.
- If one or two tokens are present, extract the available fields and set missing fields to NULL.
- If more than three tokens are present, retain the first token as `http_method` and set both `resource_path` and `protocol_version` to NULL.
- A request with more than three tokens is not malformed solely because of its token count.

An unclosed or structurally invalid quoted request makes the record malformed.

## 7. Status Code

- Extract the response status.
- Store it as an integer.
- Require exactly three numeric digits.
- Missing or invalid status makes the record malformed.

## 8. Bytes Transferred

- Store bytes as a non-negative 64-bit integer.
- Numeric values are converted to BIGINT.
- A value of `-` is converted to zero.
- A missing bytes field is converted to zero.
- A non-numeric value other than `-` is malformed.
- Negative values are invalid.

## 9. Malformed Record Handling

A record is malformed if:

1. The outer log structure cannot be parsed.
2. The host is missing or represented by `-`.
3. The timestamp is missing or invalid.
4. The request field is structurally invalid, such as an unclosed quote.
5. The status code is missing or invalid.
6. The bytes field is non-numeric and is not `-`.

Each malformed input line must be counted exactly once.

Malformed records:

- Retain their original record number and batch assignment.
- Contribute to the batch malformed count.
- Contribute to the total run malformed count.
- Are excluded from Q1, Q2 and Q3.
- Are eligible for the first-100 malformed sample output.

Missing request components alone do not make a record malformed.

## 10. Query Eligibility and Definitions

### Q1: Daily Traffic Summary

Include every valid parsed record.

Group by `log_date` and `status_code`.

Calculate:

- Request count.
- Total bytes transferred.

### Q2: Top Requested Resources

Include valid records with a non-NULL `resource_path`.

Group by exact `resource_path`.

Calculate:

- Request count.
- Total bytes transferred.
- Distinct host count.

Return the top 20 resources after aggregation.

Sort by:

1. Request count descending.
2. Resource path ascending using binary UTF-8 ordering.

For whole-run results, distinct hosts must be deduplicated across the complete dataset. Do not add per-batch distinct-host counts.

### Q3: Hourly Error Analysis

Include every valid parsed record.

Group by `log_date` and `log_hour`.

Calculate:

- Total request count.
- Error request count, where status is between 400 and 599 inclusive.
- Distinct hosts among error requests.
- Error rate: error request count divided by total request count.

Include hours with valid requests and zero errors. Do not synthesize hours with no requests.

## 11. Numeric and Aggregation Requirements

- Request counts and byte totals must use 64-bit integer-compatible representations.
- Error rate must be represented as a decimal fraction between 0 and 1.
- Cross-pipeline error-rate comparisons use an absolute tolerance of `1e-9`.
- Global distinct-host counts must be calculated from the complete set of relevant records, not by summing batch-level distinct counts.

## 12. Cross-Pipeline Equivalence

Pig, Hadoop MapReduce, MongoDB and Hive must implement the same:

- Outer record parsing.
- Timestamp validation and extraction.
- Request decomposition.
- Missing-value handling.
- Malformed-record criteria.
- Query eligibility.
- Grouping keys.
- Aggregation semantics.
- Sorting and tie-breaking.
- Output schemas.

The same parsing test cases must be used to validate all four implementations before processing the complete dataset.
