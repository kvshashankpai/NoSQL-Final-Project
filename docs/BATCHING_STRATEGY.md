# Batching Strategy

## DAS 839 - NoSQL Systems End Semester Project

## 1. Default Configuration

Default batch size: 1,000,000 input records.

Input files must be processed in this exact order:

1. `NASA_access_log_Jul95`
2. `NASA_access_log_Aug95`

The configured batch size is the maximum number of original input records in a batch.

## 2. Batch Ownership

The selected backend adapter is responsible for:

- Reading the original input files.
- Assigning continuous record numbers.
- Assigning batch IDs.
- Processing records within each batch.
- Producing batch-level analytics.
- Producing whole-run analytics.

The Python controller does not create production batch files and does not parse or transform the input records.

The controller passes the configured batch size and ordered original input paths to exactly one selected adapter.

`scripts/create_batches.py` is retained as a diagnostic utility only. It is not part of the production execution path.

## 3. Input Record Definition

One physical line represents one input record.

Input encoding: ISO-8859-1.

The two input files are read sequentially, July first and August second. Record numbering continues across the file boundary.

Line handling:

- LF terminates a record.
- For CRLF, remove the CR immediately preceding LF.
- Count a final unterminated line as a record.
- A trailing LF does not create an additional empty record.
- A blank physical line is still an input record and is treated as malformed by the parser.
- Do not otherwise modify the original record before backend parsing.

## 4. Batch Assignment

Record numbers begin at 1 and increase continuously across both input files.

Batch IDs begin at 1 and are calculated as:

`batch_id = floor((record_number - 1) / batch_size) + 1`

For a batch size of 1,000,000:

| Record number | Batch ID |
|---:|---:|
| 1 | 1 |
| 1,000,000 | 1 |
| 1,000,001 | 2 |
| 2,000,000 | 2 |
| 2,000,001 | 3 |

Every input record receives a batch ID, including malformed records.

The final batch may contain fewer records than the configured batch size. It must still be counted.

Empty input produces zero batches.

## 5. Batch Metadata

The adapter produces `batches.jsonl`, containing one record for each non-empty batch.

Each record contains:

- `batch_id`
- `record_count`: actual number of input records in the batch
- `malformed_count`: number of malformed records in the batch

`record_count` is the actual batch record count, not the configured maximum batch size.

Average batch size is:

`total input records / number of non-empty batches`

If there are no non-empty batches, average batch size is NULL.

## 6. Batch-Level and Whole-Run Analytics

Each pipeline must produce analytics at two levels:

1. Per-batch results.
2. Whole-run results across all input records.

All three analytical queries must follow the same definitions at both levels.

### Q1: Daily Traffic Summary

Group valid records by date and status code.

Calculate:

- Request count.
- Total bytes transferred.

### Q2: Top Requested Resources

Group valid records with a non-NULL resource path by exact resource path.

Calculate:

- Request count.
- Total bytes transferred.
- Distinct host count.

For whole-run results, distinct hosts must be deduplicated across the entire dataset. Do not add per-batch distinct-host counts.

Sort by request count descending, followed by resource path ascending using binary UTF-8 ordering. Select the top 20 only after global aggregation.

### Q3: Hourly Error Analysis

Group valid records by date and hour.

Calculate:

- Total request count.
- Error request count, where status is between 400 and 599 inclusive.
- Distinct hosts among error requests.
- Error rate: error request count divided by total request count.

Include hours with valid requests and zero errors. Do not generate synthetic rows for hours with no requests.

## 7. Output Convention

All pipeline output files use UTF-8 JSON Lines (JSONL).

The output directory contains:

- `batches.jsonl`
- `daily_traffic.jsonl`
- `top_resources.jsonl`
- `hourly_errors.jsonl`
- `malformed_samples.jsonl`

For per-batch analytics, `batch_id` is the positive batch identifier.

For whole-run analytics, `batch_id` is JSON null.

An empty result is represented by an empty output file.

## 8. Malformed Records

Malformed records retain their original batch assignment and contribute to the batch's `malformed_count`.

They are excluded from Q1, Q2 and Q3.

The adapter must retain the first 100 malformed samples per run in `malformed_samples.jsonl`. Each sample contains:

- Source file.
- Source record number.
- Malformation reason.
- Original raw line.

The total malformed count must still include every malformed record, not only the retained samples.

## 9. Runtime Measurement

Runtime measurement is coordinated by the Python controller.

It begins immediately before the selected adapter starts reading its input, including adapter-side input reading and batch assignment.

It includes:

- Reading the original input files.
- Batch assignment.
- Parsing and transformation.
- Query execution and aggregation.
- Writing adapter outputs.
- Output validation.
- Loading results into PostgreSQL.
- Committing the run and its results.

The timer stops after the final result transaction commits successfully. The elapsed time is then persisted in run metadata.

Dataset downloads, software installation and report rendering are excluded.

## 10. Consistency Requirement

All four implementations must use the same:

- Input file order.
- Record definition.
- Encoding and line handling.
- Record numbering.
- Batch assignment rule.
- Parsing and malformed-record rules.
- Query definitions.
- Aggregation semantics.
- Output schemas.

The four pipelines are:

1. Apache Pig
2. Hadoop MapReduce
3. MongoDB
4. Apache Hive
