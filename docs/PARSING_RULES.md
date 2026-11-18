# Common Parsing Rules

## DAS 839 - NoSQL Systems End Semester Project

## 1. Raw Input Format

Each input record follows the NASA HTTP log format:

`host ident authuser [timestamp] "request" status bytes`

Example:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

The ident and authuser fields are not required in the structured output.

Input encoding is ISO-8859-1. Each physical line is one input record. The backend adapter removes only the line terminator before parsing.

Only LF (`0x0A`) ends a line. A CR (`0x0D`) directly before LF is part of the terminator and is removed. A CR anywhere else is ordinary line content.

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

The text between the brackets must match exactly:

`^(\d{2})/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$`

Rules:

- Preserve the original timestamp string (the text inside the brackets) as `timestamp`.
- Month abbreviations are case-sensitive English (`Jul` is valid, `jul` and `JUL` are not).
- Validate strictly, with no rollover:
  - the year must be `0001` or later;
  - the day must exist in that month and year, using Gregorian leap years (`29/Feb/1996` is valid, `29/Feb/1995`, `31/Jun/1995` and `32/Jul/1995` are not);
  - hour `00`-`23`, minute `00`-`59`, second `00`-`59`;
  - offset hours `00`-`23` and offset minutes `00`-`59`.
- Derive `log_date` as `YYYY-MM-DD` and `log_hour` as an integer 0-23 directly from the digits in the string.
- Do not convert timestamps to UTC or machine-local time. The offset is validated but never applied.

Invalid or missing timestamps make the record malformed.

Implementation warnings:

- Do not derive date or hour with date-time functions that apply the offset or a session time zone. This includes Hive `unix_timestamp`/`from_unixtime`, MongoDB `$dateFromString` with `%z`, and Pig `ToDate`. Cut the fields out of the string instead.
- Java `SimpleDateFormat` is lenient by default and silently turns `32/Jul` into `01/Aug`. Any Java-based check (Hive, MapReduce, Pig UDFs) must call `setLenient(false)` or use `java.time` with `ResolverStyle.STRICT`.

## 5. Outer Record Parsing

Every backend splits a non-blank line into its outer fields using this exact pattern. It uses only syntax shared by Java regex (Pig, Hive, MapReduce) and PCRE (MongoDB):

`^([^ \t]+)[ \t]+([^ \t]+)[ \t]+([^ \t]+)[ \t]+\[([^\]]*)\][ \t]+"(.*)"[ \t]+([^ \t]+)(?:[ \t]+([^ \t]+))?[ \t]*$`

| Group | Field |
|---|---|
| 1 | host |
| 2 | ident (any token; not required to be `-`; not output) |
| 3 | authuser (any token; not required to be `-`; not output) |
| 4 | timestamp text inside the brackets |
| 5 | request text inside the quotes |
| 6 | status token |
| 7 | bytes token (optional) |

Rules:

- One or more spaces or tabs separate outer fields.
- Leading whitespace before the host does not match, so a line with a missing host is malformed.
- Trailing spaces and tabs at the end of the line are allowed and ignored.
- The request ends at the **last** `"` that is followed by a status token and an optional bytes token. Because `(.*)` is greedy, a `"` inside the request is kept as part of the request.
- Any token after the bytes token makes the outer structure invalid.
- An absent bytes field or a bytes value of `-` is interpreted as zero.

The parser must reject an invalid outer structure rather than silently accepting a partial record. Groups 4, 6 and 7 are then validated by sections 4, 7 and 8.

## 6. HTTP Request Parsing

The request is enclosed in double quotes.

Example:

`"GET /history/apollo/ HTTP/1.0"`

Trim horizontal whitespace (spaces and tabs) from the request before splitting it on runs of horizontal whitespace.

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
- If one or two tokens are present, extract the available fields and set missing fields to NULL. A single token is always the method, even if it looks like a path (for example `"/index.html"` gives `http_method = /index.html` and `resource_path = NULL`).
- The third token is the protocol whatever its text; it is not checked against `HTTP/x.y`.
- If more than three tokens are present, retain the first token as `http_method` and set both `resource_path` and `protocol_version` to NULL.
- A request with more than three tokens is not malformed solely because of its token count.

An unclosed quoted request does not match the outer pattern in section 5, so the record is malformed with reason `BAD_STRUCTURE`.

## 7. Status Code

- The status token must match `^[0-9]{3}$`.
- Store it as an integer (`000` is stored as `0`).
- Missing status fails the outer pattern (`BAD_STRUCTURE`); a status token that is not three digits is `BAD_STATUS`.

## 8. Bytes Transferred

- Store bytes as a non-negative 64-bit integer.
- A missing bytes field is converted to zero.
- A value of `-` is converted to zero.
- Otherwise the token must match `^[0-9]+$` and be at most `9223372036854775807`. Leading zeros are allowed.
- Any other value is malformed with reason `BAD_BYTES`. This includes negative values such as `-1`, non-numeric values, and values that overflow a signed 64-bit integer.

## 9. Malformed Record Handling

Checks run in this order. The first check that fails gives the record's single reason code:

| Order | Reason code | Condition |
|---|---|---|
| 1 | `BLANK_LINE` | The line is empty after terminator removal |
| 2 | `NUL_BYTE` | The line contains U+0000 (input byte `0x00`) anywhere |
| 3 | `BAD_STRUCTURE` | The line does not match the outer pattern in section 5 (includes missing host, unclosed request quote, missing status, extra trailing tokens, and lines made only of whitespace) |
| 4 | `BAD_HOST` | The host is exactly `-` |
| 5 | `BAD_TIMESTAMP` | The bracket text fails section 4 |
| 6 | `BAD_STATUS` | The status token fails section 7 |
| 7 | `BAD_BYTES` | The bytes token fails section 8 |

**NUL policy.** A record containing U+0000 is malformed even if every field would otherwise parse. Reason: PostgreSQL text columns cannot store U+0000, and a NUL inside a host or resource path would otherwise have to be changed before loading, giving different group keys in the database than in the engine. Rejecting the record keeps every engine and the database identical. The record is still counted, keeps its batch, and can appear in malformed samples (stored as described in `DATABASE_DESIGN.md` section 6).

Each malformed input line must be counted exactly once, with exactly one reason code.

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

## 11. Backend Implementation Warnings

These are known differences between the engines and this contract. Each adapter must handle them explicitly:

- **Line terminators:** Hadoop's `LineRecordReader` (used by `TextInputFormat`, Pig `PigStorage`/`TextLoader` and Hive text tables) also ends a line at a lone CR. That breaks the LF-only rule in section 1. Use a custom record delimiter (`textinputformat.record.delimiter` set to `\n`) and strip a trailing CR yourself.
- **Encoding:** Hadoop `Text`, Pig `chararray` and Python's default text mode decode input as UTF-8, not ISO-8859-1. Decode the raw bytes as ISO-8859-1 explicitly. Options: Hive table property `serialization.encoding`, `new String(bytes, ISO_8859_1)` in MapReduce, a loader UDF in Pig, and binary reads in the MongoDB loader.
- **Python readers:** open files in binary mode, or with `newline=''`. Universal-newline text mode also splits on a lone CR.
- **Integer division:** Pig and Hive integer division can truncate. Cast to a floating or decimal type before computing `error_rate`.

## 12. Numeric and Aggregation Requirements

- Request counts and byte totals must use 64-bit integer-compatible representations.
- Error rate must be represented as a decimal fraction between 0 and 1.
- Cross-pipeline error-rate comparisons use an absolute tolerance of `1e-9`.
- Global distinct-host counts must be calculated from the complete set of relevant records, not by summing batch-level distinct counts.

## 13. Cross-Pipeline Equivalence

Pig, Hadoop MapReduce, MongoDB and Hive must implement the same:

- Outer record parsing.
- Timestamp validation and extraction.
- Request decomposition.
- Missing-value handling.
- Malformed-record criteria and reason codes.
- Query eligibility.
- Grouping keys.
- Aggregation semantics.
- Sorting and tie-breaking.
- Output schemas.

The same parsing test cases must be used to validate all four implementations before processing the complete dataset.
