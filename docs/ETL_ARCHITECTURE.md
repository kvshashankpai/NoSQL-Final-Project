# ETL Architecture

## DAS 839 - NoSQL Systems End Semester Project

## 1. Objective

Build a multi-pipeline ETL and reporting framework for NASA HTTP Web Server Logs using four independent processing technologies:

1. Apache Pig
2. Hadoop MapReduce
3. MongoDB
4. Apache Hive

The objective is to compare correctness, implementation approaches, batching behaviour and execution runtime.

## 2. System Architecture

The application consists of:

- Streamlit frontend
- Python controller
- Four independent backend adapters
- PostgreSQL reporting database
- Reporting module

The user selects exactly one pipeline per run.

## 3. Component Responsibilities

### 3.1 Streamlit Frontend

- Pipeline selection
- Batch-size configuration
- Run initiation
- Execution status
- Runtime and batching statistics
- Query result visualisation
- Run history and comparison

### 3.2 Python Controller

- Validate configuration
- Generate a UUID run ID
- Record UTC start timestamp
- Start runtime measurement immediately before adapter input reading
- Invoke exactly one selected adapter
- Pass ordered original input paths, batch size and fresh output directory
- Check adapter exit status
- Validate adapter outputs
- Load all results into PostgreSQL transactionally
- Stop timing after successful result commit
- Persist elapsed runtime
- Mark the run complete only after elapsed time is saved

The controller must not perform production batching, parsing, transformation or analytical aggregation.

### 3.3 Backend Adapters

Each adapter owns production input processing:

- Read original input files in configured order
- Use ISO-8859-1 encoding
- Maintain continuous record numbering
- Assign batch IDs using the common batch formula
- Parse and classify records
- Execute Q1, Q2 and Q3 within the selected backend
- Produce per-batch and whole-run results
- Write the required JSONL output files
- Return a non-zero exit code on failure

| Pipeline | Technology | Directory |
|---|---|---|
| Pig | Apache Pig | `pipelines/pig/` |
| MapReduce | Java Hadoop MapReduce | `pipelines/mapreduce/` |
| MongoDB | MongoDB aggregation | `pipelines/mongodb/` |
| Hive | Apache Hive | `pipelines/hive/` |

Python must not replace backend processing.

### 3.4 PostgreSQL

PostgreSQL stores:

- Run metadata
- Batch statistics
- Phase timings
- Malformed samples
- Q1 daily traffic results
- Q2 top-resource results
- Q3 hourly error results

All results belonging to a run are loaded in one transaction. Failed runs must not publish partial query results.

### 3.5 Reporting Module

The reporting module reads committed PostgreSQL data and displays:

- Query result tables
- Batch-level and whole-run views
- Runtime summaries
- Malformed-record statistics
- Comparisons between pipeline runs

Report rendering is excluded from measured ETL runtime.

## 4. Input Dataset

Process the original NASA files in this order:

1. `data/raw/NASA_access_log_Jul95`
2. `data/raw/NASA_access_log_Aug95`

The original files remain unchanged.

Input encoding is ISO-8859-1. One physical line represents one input record.

## 5. Adapter Command-Line Interface

Each adapter must support:

- `--batch-size N`
- `--output-dir DIR`
- `--input FILE`

Repeat `--input` for each source file, preserving configured order.

Example argument structure:

`--batch-size 1000000 --output-dir data/work/RUN_ID/output --input data/raw/NASA_access_log_Jul95 --input data/raw/NASA_access_log_Aug95`

Adapters are launched from the repository root. Relative paths are resolved from the repository root. Each execution receives a fresh output directory.

The exact executable differs by backend, but the argument contract remains common.

## 6. End-to-End Execution Workflow

1. User selects a pipeline and batch size.
2. Controller validates the configuration.
3. Controller generates a UUID run ID and records UTC start time.
4. Controller creates a fresh run-specific output directory.
5. Runtime measurement starts immediately before adapter input reading.
6. Controller invokes exactly one adapter.
7. Adapter reads inputs, assigns record numbers and batches, parses records and executes the queries.
8. Adapter writes JSONL outputs.
9. Controller checks exit status and validates outputs.
10. Controller loads run metadata, batch records, malformed samples and query results in one PostgreSQL transaction.
11. Transaction commits successfully.
12. Runtime measurement stops.
13. Elapsed time is persisted in run metadata.
14. Run is marked complete only after elapsed time is saved.
15. Streamlit displays the completed run.

## 7. Failure Handling

A run must not be marked complete if:

- Adapter execution fails
- Adapter returns a non-zero exit code
- Required output files are missing or invalid
- PostgreSQL loading fails
- Result transaction does not commit
- Elapsed runtime cannot be persisted

Failed runs must not appear as completed benchmark results.

## 8. Runtime Measurement

Runtime includes:

- Input reading
- Record numbering and batch assignment
- Parsing and transformation
- Query execution and aggregation
- Adapter output generation
- Output validation
- PostgreSQL loading and commit

Runtime stops after the result transaction commits successfully. Elapsed time is then persisted.

Dataset downloads, software installation, report rendering and pre-run user interaction are excluded.

## 9. Production Batching

Production batching belongs to the selected backend adapter.

The controller passes the configured batch size and original input paths.

The common formula is:

`batch_id = floor((record_number - 1) / batch_size) + 1`

Record numbering continues across July and August.

`scripts/create_batches.py` is a diagnostic utility only. It is not part of the production execution path.

## 10. Analytical Workload

All four pipelines implement:

### Q1: Daily Traffic Summary

Group valid records by date and status code. Calculate request count and total bytes.

### Q2: Top Requested Resources

Group valid records with a non-NULL resource path. Calculate request count, total bytes and distinct hosts. Return the global top 20 after aggregation with deterministic tie-breaking.

### Q3: Hourly Error Analysis

Group valid records by date and hour. Calculate total requests, error requests, distinct error hosts and error rate. Error statuses range from 400 through 599 inclusive.

## 11. Output Contract

Each adapter produces UTF-8 JSONL files:

- `batches.jsonl`
- `daily_traffic.jsonl`
- `top_resources.jsonl`
- `hourly_errors.jsonl`
- `malformed_samples.jsonl`

Analytics files contain per-batch and whole-run rows. Per-batch rows have a positive batch ID; whole-run rows have a NULL batch ID.

Schemas are defined in `COMMON_DATA_MODEL.md`.

## 12. Correctness and Equivalence

All four implementations must be validated against the same parsing and batching test cases.

Compare:

- Valid and malformed record counts
- Parsed field values
- Batch assignments
- Per-batch query results
- Whole-run query results
- Distinct-host counts
- Top-20 ordering
- Error rates within absolute tolerance `1e-9`

Use the same dataset, batch size and query semantics for runtime comparisons.

## 13. Supporting Documentation

- `COMMON_DATA_MODEL.md`
- `BATCHING_STRATEGY.md`
- `PARSING_RULES.md`
- `PARSING_TEST_CASES.md`
