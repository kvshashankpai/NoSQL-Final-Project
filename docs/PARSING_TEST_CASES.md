# Common Parsing Test Cases

## DAS 839 - NoSQL Systems End Semester Project

All four pipelines must produce equivalent outcomes for these test cases.

Notation used in the inputs below:

- `<TAB>` is one horizontal tab character (`0x09`).
- `<CR>` is one carriage return (`0x0D`).
- `<NUL>` is one zero byte (`0x00`).
- `<E9>` is the single byte `0xE9`. In ISO-8859-1 this is `é`, written to JSON as UTF-8.

Unless stated otherwise, the host is `199.72.81.55`, the timestamp is `01/Jul/1995:00:00:01 -0400`, and the expected `log_date` and `log_hour` are `1995-07-01` and `0`.

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
| T08 | Invalid timestamp | Malformed, `BAD_TIMESTAMP` |
| T09 | Missing host | Malformed, `BAD_STRUCTURE` |
| T10 | Host represented by `-` | Malformed, `BAD_HOST` |
| T11 | Missing status code | Malformed, `BAD_STRUCTURE` |
| T12 | Non-numeric status code | Malformed, `BAD_STATUS` |
| T13 | Non-numeric bytes other than `-` | Malformed, `BAD_BYTES` |
| T14 | Unclosed request quote | Malformed, `BAD_STRUCTURE` |
| T15 | Request with more than three tokens | Valid; retain method, set path and protocol = NULL |
| T16 | HTTP status 404 | Valid; error for Q3 |
| T17 | HTTP status 500 | Valid; error for Q3 |
| T18 | HTTP status 399 | Valid; not an error for Q3 |
| T19 | HTTP status 599 | Valid; error for Q3 |
| T20 | HTTP status 600 | Valid; not an error for Q3 |
| T21 | Empty request | Valid; all request fields = NULL |
| T22 | Request with one token | Valid; method extracted |
| T23 | Request with two tokens | Valid; method and path extracted |
| T24 | Horizontal tabs between outer fields | Valid |
| T25 | Horizontal tabs between request tokens | Valid; split using horizontal whitespace |
| T26 | Negative bytes value | Malformed, `BAD_BYTES` |
| T27 | Blank physical input line | Malformed, `BLANK_LINE` |
| T28 | Invalid calendar date (day 32) | Malformed, `BAD_TIMESTAMP` |
| T29 | Invalid hour | Malformed, `BAD_TIMESTAMP` |
| T30 | Bytes value larger than signed 64-bit maximum | Malformed, `BAD_BYTES` |
| T31 | Double quote inside the request | Valid; quote kept in the path |
| T32 | Trailing spaces and tabs after bytes | Valid |
| T33 | Extra token after bytes | Malformed, `BAD_STRUCTURE` |
| T34 | Last second of the day with `-0400` offset | Valid; date and hour are not shifted |
| T35 | 31 June | Malformed, `BAD_TIMESTAMP` |
| T36 | 29 February in a non-leap year (1995) | Malformed, `BAD_TIMESTAMP` |
| T37 | 29 February in a leap year (1996) | Valid |
| T38 | Lowercase month abbreviation | Malformed, `BAD_TIMESTAMP` |
| T39 | Invalid time-zone offset minutes | Malformed, `BAD_TIMESTAMP` |
| T40 | Lone CR inside a line | One record; valid; CR kept in the path |
| T41 | Non-ASCII byte in the path | Valid; byte decoded as ISO-8859-1 |
| T42 | Line consisting of a NUL byte | Malformed, `NUL_BYTE`; sample stored with `\u0000` |
| T43 | Ident and authuser are not `-` | Valid |
| T44 | Lowercase method | Valid; method case preserved |
| T45 | Line containing only spaces | Malformed, `BAD_STRUCTURE` |
| T46 | Bytes with leading zeros | Valid; bytes = 100 |
| T47 | Single path-like request token | Valid; token becomes the method |
| T48 | Host `-` and invalid status together | Malformed, `BAD_HOST` (precedence) |
| T49 | NUL byte inside the path of an otherwise valid line | Malformed, `NUL_BYTE` |
| T50 | NUL byte in a line that is also structurally invalid | Malformed, `NUL_BYTE` (precedence) |

## 2. Sample Records and Expected Results

### T01 - Standard record

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

Expected: valid; host = `199.72.81.55`; timestamp = `01/Jul/1995:00:00:01 -0400`; log_date = `1995-07-01`; log_hour = `0`; http_method = `GET`; resource_path = `/history/apollo/`; protocol_version = `HTTP/1.0`; status_code = `200`; bytes_transferred = `6245`.

### T02 - POST request

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "POST /cgi-bin/imagemap/countdown HTTP/1.0" 200 97`

Expected: valid; http_method = `POST`; resource_path = `/cgi-bin/imagemap/countdown`; protocol_version = `HTTP/1.0`; status_code = `200`; bytes_transferred = `97`.

### T03 - Bytes represented by hyphen

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 -`

Expected: valid; bytes_transferred = `0`.

### T04 - Bytes field absent

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200`

Expected: valid; bytes_transferred = `0`.

### T05 - Request represented by hyphen

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "-" 200 0`

Expected: valid; http_method, resource_path and protocol_version are NULL.

### T06 - Query parameters

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /search?q=nasa HTTP/1.0" 200 100`

Expected: valid; resource_path = `/search?q=nasa`.

### T07 - Missing protocol

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/" 200 100`

Expected: valid; http_method = `GET`; resource_path = `/history/apollo/`; protocol_version = NULL.

### T08 - Invalid timestamp

`199.72.81.55 - - [INVALID] "GET /history/apollo/ HTTP/1.0" 200 6245`

Expected: malformed, `BAD_TIMESTAMP`.

### T09 - Missing host

` - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

(The line starts with a space.) Expected: malformed, `BAD_STRUCTURE`.

### T10 - Host represented by hyphen

`- - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245`

Expected: malformed, `BAD_HOST`.

### T11 - Missing status

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0"`

Expected: malformed, `BAD_STRUCTURE`.

### T12 - Invalid status

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" ABC 6245`

Expected: malformed, `BAD_STATUS`.

### T13 - Invalid bytes

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 abc`

Expected: malformed, `BAD_BYTES`.

### T14 - Unclosed request quote

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0 200 6245`

Expected: malformed, `BAD_STRUCTURE`.

### T15 - More than three request tokens

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0 EXTRA" 200 6245`

Expected: valid; http_method = `GET`; resource_path = NULL; protocol_version = NULL. Counts in Q1 and Q3, not in Q2.

### T16 to T20 - Status boundaries

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" <STATUS> 100`

| ID | `<STATUS>` | Q3 error? |
|---|---|---|
| T16 | `404` | Yes |
| T17 | `500` | Yes |
| T18 | `399` | No |
| T19 | `599` | Yes |
| T20 | `600` | No |

All are valid.

### T21 - Empty request

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "" 200 0`

Expected: valid; all three request fields NULL.

### T22 - One request token

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET" 200 100`

Expected: valid; http_method = `GET`; resource_path = NULL; protocol_version = NULL.

### T23 - Two request tokens

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history" 200 100`

Expected: valid; http_method = `GET`; resource_path = `/history`; protocol_version = NULL.

### T24 - Tabs between outer fields

`199.72.81.55<TAB>-<TAB>-<TAB>[01/Jul/1995:00:00:01 -0400]<TAB>"GET /history/apollo/ HTTP/1.0"<TAB>200<TAB>6245`

Expected: valid; same fields as T01.

### T25 - Tabs between request tokens

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET<TAB>/history/apollo/<TAB>HTTP/1.0" 200 6245`

Expected: valid; http_method = `GET`; resource_path = `/history/apollo/`; protocol_version = `HTTP/1.0`.

### T26 - Negative bytes

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 -1`

Expected: malformed, `BAD_BYTES`.

### T27 - Blank physical line

An empty line between two valid lines. Expected: one input record with its normal record number and batch ID; malformed, `BLANK_LINE`.

### T28 - Day 32

`199.72.81.55 - - [32/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 100`

Expected: malformed, `BAD_TIMESTAMP`. It must not roll over to 1 August.

### T29 - Hour 25

`199.72.81.55 - - [01/Jul/1995:25:00:00 -0400] "GET /history HTTP/1.0" 200 100`

Expected: malformed, `BAD_TIMESTAMP`.

### T30 - Bytes overflow

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 9223372036854775808`

Expected: malformed, `BAD_BYTES`.

### T31 - Quote inside the request

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /a"b HTTP/1.0" 200 100`

Expected: valid; http_method = `GET`; resource_path = `/a"b`; protocol_version = `HTTP/1.0`.

### T32 - Trailing whitespace

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 100  <TAB>`

Expected: valid; bytes_transferred = `100`.

### T33 - Extra token after bytes

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 100 extra`

Expected: malformed, `BAD_STRUCTURE`.

### T34 - No time-zone shift

`199.72.81.55 - - [01/Jul/1995:23:59:59 -0400] "GET /history HTTP/1.0" 200 100`

Expected: valid; log_date = `1995-07-01`; log_hour = `23`. Converting to UTC would wrongly give `1995-07-02` hour `3`. Converting to IST would also give a wrong date or hour.

### T35 to T39 - Timestamp validation

`199.72.81.55 - - [<TIMESTAMP>] "GET /history HTTP/1.0" 200 100`

| ID | `<TIMESTAMP>` | Expected |
|---|---|---|
| T35 | `31/Jun/1995:12:00:00 -0400` | Malformed, `BAD_TIMESTAMP` |
| T36 | `29/Feb/1995:12:00:00 -0400` | Malformed, `BAD_TIMESTAMP` |
| T37 | `29/Feb/1996:12:00:00 -0400` | Valid; log_date = `1996-02-29`; log_hour = `12` |
| T38 | `01/jul/1995:12:00:00 -0400` | Malformed, `BAD_TIMESTAMP` |
| T39 | `01/Jul/1995:12:00:00 -0460` | Malformed, `BAD_TIMESTAMP` |

### T40 - Lone CR inside a line

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /a<CR>b HTTP/1.0" 200 100`

Expected: exactly one input record; valid; resource_path = `/a` + CR + `b`. Hadoop's default line reader splits this into two records, which is wrong (see `PARSING_RULES.md` section 11).

### T41 - Non-ASCII byte

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /caf<E9> HTTP/1.0" 200 100`

Expected: valid; resource_path = `/café` (U+00E9). A UTF-8 decoder would produce U+FFFD instead, which is wrong.

### T42 - NUL byte

A line consisting of `<NUL>` only.

Expected: malformed, `NUL_BYTE`. JSONL `raw_line` = `"\u0000"`. Stored in PostgreSQL as the six characters `\u0000`.

### T43 - Ident and authuser present

`199.72.81.55 ident1 frank [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 100`

Expected: valid; host = `199.72.81.55`.

### T44 - Lowercase method

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "get /history HTTP/1.0" 200 100`

Expected: valid; http_method = `get`.

### T45 - Whitespace-only line

A line of three spaces. Expected: malformed, `BAD_STRUCTURE` (not `BLANK_LINE`, because the line is not empty).

### T46 - Leading zeros in bytes

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 0100`

Expected: valid; bytes_transferred = `100`.

### T47 - Single path-like token

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "/index.html" 200 100`

Expected: valid; http_method = `/index.html`; resource_path = NULL; protocol_version = NULL.

### T48 - Reason precedence

`- - - [01/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" ABC 100`

Expected: malformed, `BAD_HOST`. The host check comes before the status check.

### T49 - NUL inside a valid line

`199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /a<NUL>b HTTP/1.0" 200 100`

Expected: malformed, `NUL_BYTE`. The record would otherwise parse, but PostgreSQL cannot store U+0000 in `resource_path` (`PARSING_RULES.md` section 9, NUL policy). It is excluded from Q1, Q2 and Q3.

### T50 - NUL precedence

`<NUL> garbage`

Expected: malformed, `NUL_BYTE`. The NUL check comes before the structure check.

## 3. Batch and Record-Numbering Tests

| ID | Condition | Expected outcome |
|---|---|---|
| B01 | First input record | Record number 1, batch ID 1 |
| B02 | Record number equals batch size | Batch ID 1 |
| B03 | Record number is batch size + 1 | Batch ID 2 |
| B04 | Record numbering crosses from July to August | Continuous numbering |
| B05 | Malformed record inside a batch | Retains its batch assignment |
| B06 | Final partial batch | Counted as a non-empty batch |
| B07 | Empty input | Zero batches; average batch size NULL |
| B08 | Final line without LF | Counted as one record |
| B09 | File ending with LF | No extra record created |
| B10 | CRLF input | Remove the CRLF terminator only |
| B11 | Lone CR inside a line | Not a terminator; one record (see T40) |
| B12 | July file's last line has no LF, August follows | Two separate records, numbered n and n+1 |

## 4. Query Fixture

This fixture tests batching, malformed handling, distinct-host deduplication across batches, the Q2 tie-break, NULL paths and zero-error hours together. Run it with **batch size 3**.

Input (7 physical lines; line 5 is empty):

```text
h1 - - [01/Jul/1995:10:00:00 -0400] "GET /a HTTP/1.0" 200 100
h2 - - [01/Jul/1995:10:05:00 -0400] "GET /a HTTP/1.0" 404 -
h1 - - [01/Jul/1995:10:10:00 -0400] "GET /b HTTP/1.0" 200 50
h3 - - [01/Jul/1995:11:00:00 -0400] "GET /b HTTP/1.0" 500 10

h1 - - [02/Jul/1995:00:00:00 -0400] "GET /a HTTP/1.0" 200 100
h2 - - [02/Jul/1995:00:30:00 -0400] "-" 404 0
```

### Expected `batches.jsonl`

| batch_id | record_count | malformed_count |
|---:|---:|---:|
| 1 | 3 | 0 |
| 2 | 3 | 1 |
| 3 | 1 | 0 |

Run level: total_records = 7, total_batches = 3, malformed_count = 1, average_batch_size = 2.333333...

### Expected `malformed_samples.jsonl`

| source_file | record_number | batch_id | reason | raw_line |
|---|---:|---:|---|---|
| (fixture file name) | 5 | 2 | `BLANK_LINE` | `""` |

### Expected Q1 `daily_traffic.jsonl`

| batch_id | log_date | status_code | request_count | total_bytes |
|---|---|---:|---:|---:|
| 1 | 1995-07-01 | 200 | 2 | 150 |
| 1 | 1995-07-01 | 404 | 1 | 0 |
| 2 | 1995-07-01 | 500 | 1 | 10 |
| 2 | 1995-07-02 | 200 | 1 | 100 |
| 3 | 1995-07-02 | 404 | 1 | 0 |
| null | 1995-07-01 | 200 | 2 | 150 |
| null | 1995-07-01 | 404 | 1 | 0 |
| null | 1995-07-01 | 500 | 1 | 10 |
| null | 1995-07-02 | 200 | 1 | 100 |
| null | 1995-07-02 | 404 | 1 | 0 |

### Expected Q2 `top_resources.jsonl`

| batch_id | rank | resource_path | request_count | total_bytes | distinct_host_count |
|---|---:|---|---:|---:|---:|
| 1 | 1 | /a | 2 | 100 | 2 |
| 1 | 2 | /b | 1 | 50 | 1 |
| 2 | 1 | /a | 1 | 100 | 1 |
| 2 | 2 | /b | 1 | 10 | 1 |
| null | 1 | /a | 3 | 200 | 2 |
| null | 2 | /b | 2 | 60 | 2 |

Batch 2 is a tie on request count, broken by path order. Batch 3 has no rows, because its only valid record has a NULL path. The whole-run `/a` row has 2 distinct hosts, not 2 + 1 = 3.

### Expected Q3 `hourly_errors.jsonl`

| batch_id | log_date | log_hour | error_request_count | total_request_count | error_rate | distinct_error_hosts |
|---|---|---:|---:|---:|---:|---:|
| 1 | 1995-07-01 | 10 | 1 | 3 | 0.333333333333 | 1 |
| 2 | 1995-07-01 | 11 | 1 | 1 | 1 | 1 |
| 2 | 1995-07-02 | 0 | 0 | 1 | 0 | 0 |
| 3 | 1995-07-02 | 0 | 1 | 1 | 1 | 1 |
| null | 1995-07-01 | 10 | 1 | 3 | 0.333333333333 | 1 |
| null | 1995-07-01 | 11 | 1 | 1 | 1 | 1 |
| null | 1995-07-02 | 0 | 1 | 2 | 0.5 | 1 |

Batch 2, hour 0 on 2 July, is the zero-error row that must still appear.

## 5. Important Validation Requirement

These cases define the shared parsing and batching contract.

Every implementation must be tested against the same cases before processing the complete NASA dataset. The executable versions are in `tests/fixtures/` (see `tests/fixtures/README.md`), with the exact input bytes and the expected output files.

A test passes only when the backend produces the expected validity, reason code, extracted fields, record metadata and query results.

Cross-pipeline comparison must verify:

- Identical valid and malformed record classification and reason codes.
- Identical parsed field values.
- Identical batch assignments.
- Identical per-batch analytics.
- Identical whole-run analytics.
- Identical malformed counts.

These test cases specify expected behaviour. They do not imply that all four implementations have already passed them.
