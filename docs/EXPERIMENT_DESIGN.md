# Experiment and Benchmarking Design

## DAS 839 - NoSQL Systems End-Semester Project

## 1. Objective

This document defines a consistent method for evaluating the four ETL
pipelines:

1.  Apache Pig
2.  Hadoop MapReduce
3.  MongoDB
4.  Apache Hive

The evaluation covers functional correctness, batch behaviour, execution
runtime, and resource/performance observations where available.

The purpose is to compare the implementations under equivalent input and
workload conditions. A runtime comparison is meaningful only after the
outputs have passed correctness validation.

## 2. Experimental Principles

All pipeline comparisons must use the same:

-   Original NASA input files.
-   Input file order: July followed by August.
-   Input encoding and record definition.
-   Batch size.
-   Parsing and malformed-record rules.
-   Query definitions and aggregation semantics.
-   Output schemas.
-   Runtime measurement boundary.

Record the software versions and relevant machine/environment details
for every benchmark session.

Do not compare runs performed with materially different input data or
configuration as if they were equivalent experiments.

## 3. Dataset and Input Verification

Use the original NASA HTTP Web Server Logs:

1.  `NASA_access_log_Jul95`
2.  `NASA_access_log_Aug95`

Keep the source files unchanged.

Before benchmarking, record:

-   Input filenames.
-   File sizes in bytes.
-   SHA-256 checksum of each input file.
-   Input order.
-   Encoding: ISO-8859-1.
-   Configured batch size.

A checksum change means the input data may have changed; such runs must
not be treated as directly comparable without explanation.

This verification step runs once per benchmark session, outside any
timed run. Its results are copied into `runs.input_files` for every run.

## 4. Batch-Size Experiments

Use the following batch sizes:

  Experiment     Batch size
  ------------ ------------
  B1                100,000
  B2                500,000
  B3              1,000,000

Each backend adapter owns production input reading and batch assignment.

For every experiment, record the actual number of non-empty batches,
total input records, malformed records, and average batch size.

The final partial batch must be included.

The batch assignment rule is:

`batch_id = floor((record_number - 1) / batch_size) + 1`

Record numbering is continuous across the July and August files.

## 5. Repeated Trials

Run each pipeline and batch-size combination at least three times after
the implementation has passed correctness checks.

For each combination, record:

-   Trial number.
-   Pipeline.
-   Batch size.
-   Start timestamp.
-   Elapsed runtime in seconds.
-   Total records.
-   Number of batches.
-   Malformed-record count.
-   Run ID.
-   Success or failure status.
-   Relevant environment notes.

Use the median of the three or more successful trial runtimes as the
primary reported runtime.

Do not silently discard failed or unusually slow trials. Record failures
and explain any excluded measurement.

Where possible, keep machine load and background activity reasonably
consistent across trials.

## 6. Runtime Measurement Boundary

The controller cannot see when an adapter begins reading, so it starts
the timer just before it launches the adapter.

The measured duration includes:

-   Backend startup (JVM, Hadoop, database connections).
-   Input reading.
-   Record numbering and batch assignment.
-   Parsing and transformation.
-   Query execution and aggregation.
-   Adapter output generation.
-   Output validation.
-   PostgreSQL loading and result transaction commit.

The timer stops after the final result transaction commits successfully.

The elapsed duration is then saved and the run is marked `COMPLETED` in
one statement. Failed runs are kept with status `FAILED`.

Exclude:

-   Dataset downloads.
-   Software installation.
-   Input checksum verification.
-   Report rendering.
-   Time spent waiting for user interaction before the run starts.

Use the same timing boundary for all four backends.

## 7. Correctness Validation

Correctness validation must be completed before runtime results are used
for performance conclusions.

### 7.1 Parsing and Batching

Run the shared parsing and batching test cases against every backend.

Verify:

-   Valid and malformed record classifications.
-   Extracted field values.
-   Source file and continuous record numbers.
-   Batch IDs.
-   Final partial batch handling.
-   CRLF and LF handling.
-   Final unterminated line handling.
-   Malformed sample content and ordering.

### 7.2 Query Results

Compare the per-batch and whole-run results for:

-   Q1 daily traffic.
-   Q2 top requested resources.
-   Q3 hourly error analysis.

Check all grouping keys, counts, byte totals, distinct-host counts, and
output schemas.

For Q2, verify the top-20 ranking and deterministic tie-breaking.
Whole-run distinct-host counts must be deduplicated across the entire
dataset.

For Q3, compare error rates using an absolute tolerance of `1e-9`.

### 7.3 Comparison Method

Where practical, compare normalized result datasets in both directions
so that missing rows and extra rows are detected.

A pipeline must not be described as equivalent merely because total row
counts match.

Record validation outcomes, differences, and unresolved issues.

## 8. Results Recording Template

Use a results table with at least these columns:

  ---------------------------------------------------------------------------------------
  Pipeline       Batch    Trial Run ID   Runtime   Records   Batches   Malformed Status
                  size                       (s)
  ----------- -------- -------- ------ --------- --------- --------- ----------- --------
  Pig           100000        1

  MapReduce     100000        1

  MongoDB       100000        1

  Hive          100000        1
  ---------------------------------------------------------------------------------------

Add rows for every trial and batch size. Do not fill in results until
the corresponding run has actually completed.

## 9. Summary and Reporting

For each pipeline and batch size, report:

-   Number of successful trials.
-   Median runtime.
-   Minimum and maximum runtime.
-   Correctness validation status.
-   Total records and batches processed.
-   Malformed-record count.

Use tables and charts to compare median runtimes across pipelines and
batch sizes.

Explain observed differences in the context of the implementations and
execution environment. Avoid claiming that one technology is universally
faster based on this dataset alone.

## 10. Reproducibility Checklist

Before accepting a benchmark result, confirm:

-   [ ] Correct July and August input files were used.
-   [ ] Input checksums were recorded.
-   [ ] Input order was July then August.
-   [ ] Correct batch size was recorded.
-   [ ] Backend implementation passed shared correctness tests.
-   [ ] All three analytical queries were produced.
-   [ ] Per-batch and whole-run results were checked.
-   [ ] Runtime boundary was consistent.
-   [ ] Run ID and environment details were recorded.
-   [ ] Trial outcome was retained, including failures.
-   [ ] Median was calculated from at least three successful trials.

## 11. Interpretation Limitations

Benchmark results describe the tested implementations, dataset,
configuration, and environment.

They do not establish universal performance rankings for Pig, Hadoop
MapReduce, MongoDB, or Hive.

Do not report unexecuted experiments as completed. Clearly distinguish
planned experiments, completed runs, failed runs, and validated results.
