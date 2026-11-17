# Common Parsing Test Cases

All four pipelines must produce equivalent outcomes
for these test cases.

| ID | Input condition | Expected outcome |
|---|---|---|
| T01 | Standard GET request | Valid; extract all fields |
| T02 | Standard POST request | Valid; extract all fields |
| T03 | Bytes represented by '-' | Valid; bytes = 0 |
| T04 | Bytes field absent | Valid; bytes = 0 |
| T05 | Request represented by '-' | Valid; request fields = NULL |
| T06 | Request with query parameters | Valid; preserve complete path |
| T07 | Request without protocol | Valid; protocol = NULL |
| T08 | Invalid timestamp | Malformed |
| T09 | Missing host | Malformed |
| T10 | Host represented by '-' | Malformed |
| T11 | Missing status code | Malformed |
| T12 | Non-numeric status code | Malformed |
| T13 | Non-numeric bytes other than '-' | Malformed |
| T14 | Unclosed request quote | Malformed |
| T15 | Request with more than 3 components | Malformed |
| T16 | HTTP status 404 | Valid; error for Q3 |
| T17 | HTTP status 500 | Valid; error for Q3 |
| T18 | HTTP status 399 | Valid; not an error for Q3 |
| T19 | HTTP status 599 | Valid; error for Q3 |
| T20 | HTTP status 600 | Valid; not an error for Q3 |

## Sample Records

T01 - Standard record:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

Expected:
- host = 199.72.81.55
- log_date = 1995-07-01
- log_hour = 0
- http_method = GET
- resource_path = /history/apollo/
- protocol_version = HTTP/1.0
- status_code = 200
- bytes_transferred = 6245

T03 - Missing bytes:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 -

Expected:
- Valid
- bytes_transferred = 0

T05 - Missing request:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "-" 200 0

Expected:
- Valid
- http_method = NULL
- resource_path = NULL
- protocol_version = NULL

T06 - Query parameters:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /search?q=nasa HTTP/1.0" 200 100

Expected:
- resource_path = /search?q=nasa

T07 - Missing protocol:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/" 200 100

Expected:
- Valid
- http_method = GET
- resource_path = /history/apollo/
- protocol_version = NULL

T08 - Invalid timestamp:

199.72.81.55 - - [INVALID] "GET /history/apollo/ HTTP/1.0" 200 6245

Expected:
- Malformed

T12 - Invalid status:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" ABC 6245

Expected:
- Malformed

T14 - Unclosed request:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0 200 6245

Expected:
- Malformed

## Important

These cases define the shared parsing contract.

Every implementation must be tested against the same
valid and malformed inputs before processing the
complete NASA dataset.
