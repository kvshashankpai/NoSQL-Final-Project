# Common Parsing Test Cases

## DAS 839 - NoSQL Systems End Semester Project

All four pipelines must produce equivalent outcomes for these test cases.

## 1. Test Case Summary

| ID | Input condition | Expected outcome |
|---|---|---|
| T01 | Standard GET request | Valid; extract all fields |
| T02 | Standard POST request | Valid; extract all fields |
| T03 | Bytes represented by `-` | Valid; bytes = 0 |
| T04 | Bytes field absent | Valid; bytes = 0 |
| T05 | Request represented by `-` | Valid; all request fields = NULL |
| T06 | Request with query parameters | Valid; preserve complete path |
| T07 | Request without protocol | Valid; protocol = NULL |
| T08 | Invalid timestamp | Malformed |
| T09 | Missing host | Malformed |
| T10 | Host represented by `-` | Malformed |
| T11 | Missing status code | Malformed |
| T12 | Non-numeric status code | Malformed |
| T13 | Non-numeric bytes other than `-` | Malformed |
| T14 | Unclosed request quote | Malformed |
| T15 | Request with more than three tokens | Valid; retain method, set path and protocol = NULL |
| T16 | HTTP status 404 | Valid; error for Q3 |
| T17 | HTTP status 500 | Valid; error for Q3 |
| T18 | HTTP status 399 | Valid; not an error for Q3 |
| T19 | HTTP status 599 | Valid; error for Q3 |
| T20 | HTTP status 600 | Valid; not an error for Q3 |
| T21 | Empty request | Valid; all request fields = NULL |
| T22 | Request with one token | Valid; method extracted |
| T23 | Request with two tokens | Valid; method and path extracted |
| T24 | Horizontal tabs between outer fields | Valid if all required fields are present |
| T25 | Horizontal tabs between request tokens | Valid; split using horizontal whitespace |
| T26 | Negative bytes value | Malformed |
| T27 | Blank physical input line | Malformed |
| T28 | Invalid calendar date | Malformed |
| T29 | Invalid hour, minute or second | Malformed |
| T30 | Bytes value larger than signed 64-bit maximum | Malformed |

## 2. Sample Records and Expected Results

### T01 - Standard record

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

Expected:

- Valid.
- host = `199.72.81.55`
- log_date = `1995-07-01`
- log_hour = `0`
- http_method = `GET`
- resource_path = `/history/apollo/`
- protocol_version = `HTTP/1.0`
- status_code = `200`
- bytes_transferred = `6245`

### T03 - Missing bytes represented by hyphen

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 -`

Expected:

- Valid.
- bytes_transferred = `0`

### T04 - Missing bytes field

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200`

Expected:

- Valid.
- bytes_transferred = `0`

### T05 - Missing request

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "-" 200 0`

Expected:

- Valid.
- http_method = NULL
- resource_path = NULL
- protocol_version = NULL

### T06 - Query parameters

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /search?q=nasa HTTP/1.0" 200 100`

Expected:

- Valid.
- resource_path = `/search?q=nasa`

### T07 - Missing protocol

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/" 200 100`

Expected:

- Valid.
- http_method = `GET`
- resource_path = `/history/apollo/`
- protocol_version = NULL

### T08 - Invalid timestamp

Input:

`199.72.81.55 - - [INVALID] "GET /history/apollo/ HTTP/1.0" 200 6245`

Expected:

- Malformed.

### T10 - Missing host represented by hyphen

Input:

`- - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

Expected:

- Malformed.

### T12 - Invalid status

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" ABC 6245`

Expected:

- Malformed.

### T13 - Invalid bytes

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 abc`

Expected:

- Malformed.

### T14 - Unclosed request quote

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0 200 6245`

Expected:

- Malformed.

### T15 - More than three request tokens

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0 EXTRA" 200 6245`

Expected:

- Valid.
- http_method = `GET`
- resource_path = NULL
- protocol_version = NULL

### T16 - HTTP status 404

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /missing HTTP/1.0" 404 100`

Expected:

- Valid.
- Q3 error request.

### T18 - HTTP status 399

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 399 100`

Expected:

- Valid.
- Not an error request for Q3.

### T20 - HTTP status 600

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 600 100`

Expected:

- Valid.
- Not an error request for Q3.

### T21 - Empty request

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "" 200 0`

Expected:

- Valid.
- http_method = NULL
- resource_path = NULL
- protocol_version = NULL

### T22 - One request token

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET" 200 100`

Expected:

- Valid.
- http_method = `GET`
- resource_path = NULL
- protocol_version = NULL

### T23 - Two request tokens

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history" 200 100`

Expected:

- Valid.
- http_method = `GET`
- resource_path = `/history`
- protocol_version = NULL

### T24 - Horizontal tabs between outer fields

Use horizontal tab characters instead of spaces between the host, timestamp, request, status and bytes fields.

Expected:

- Valid if the remaining fields are valid.
- Extract all fields normally.

### T25 - Horizontal tabs between request tokens

Input request content:

`GET	/history/apollo/	HTTP/1.0`

Expected:

- Valid.
- http_method = `GET`
- resource_path = `/history/apollo/`
- protocol_version = `HTTP/1.0`

### T26 - Negative bytes

Input:

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 -1`

Expected:

- Malformed.

### T27 - Blank physical line

Input:

An empty physical line in the source file.

Expected:

- Count as one input record.
- Assign its normal record number and batch ID.
- Mark it malformed.

### T28 - Invalid calendar date

Input:

`199.72.81.55 - - [32/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 100`

Expected:

- Malformed.

### T29 - Invalid time

Input:

`199.72.81.55 - - [01/Jul/1995:25:00:00 -0400] "GET /history HTTP/1.0" 200 100`

Expected:

- Malformed.

### T30 - Bytes overflow

Input:

A record with a valid outer structure and bytes value greater than `9223372036854775807`.

Expected:

- Malformed because the value cannot be represented as a signed 64-bit integer.

## 3. Batch and Record-Numbering Tests

The following tests validate the common input and batching contract.

| ID | Condition | Expected outcome |
|---|---|---|
| B01 | First input record | Record number 1, batch ID 1 |
| B02 | Record number equals batch size | Batch ID 1 |
| B03 | Record number is batch size + 1 | Batch ID 2 |
| B04 | Record numbering crosses from July to August | Continuous numbering |
| B05 | Malformed record inside a batch | Retains its batch assignment |
| B06 | Final partial batch | Counted as a non-empty batch |
| B07 | Empty input | Zero batches |
| B08 | Final line without LF | Counted as one record |
| B09 | File ending with LF | No extra record created |
| B10 | CRLF input | Remove CRLF terminator only |

## 4. Important Validation Requirement

These cases define the shared parsing and batching contract.

Every implementation must be tested against the same cases before processing the complete NASA dataset.

A test passes only when the backend produces the expected validity, extracted fields, record metadata and query eligibility.

Cross-pipeline comparison must verify:

- Identical valid and malformed record classification.
- Identical parsed field values.
- Identical batch assignments.
- Identical per-batch analytics.
- Identical whole-run analytics.
- Identical malformed counts.

These test cases specify expected behaviour; they do not imply that all four implementations have already passed them.
