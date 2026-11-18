# Implementation Roadmap

## DAS 839 - NoSQL Systems End-Semester Project

## 1. Purpose

This roadmap organizes implementation of the multi-pipeline ETL and
reporting framework. It defines the recommended order of work, expected
deliverables, and validation gates.

The four backend pipelines are Apache Pig, Hadoop MapReduce, MongoDB,
and Apache Hive. The Python controller orchestrates one selected
pipeline per run, PostgreSQL stores common reporting results, and
Streamlit provides the user interface.

This document is a plan. A task must only be marked complete after its
implementation and validation have actually been performed.

The stages below are internal work stages. They are not the course's
two review phases. The course's **Phase 1 review** needs the
architecture plus a working prototype with one or two pipelines, which
means reaching Stage 5 for at least one or two backends (with enough of
Stages 3, 4 and 6 to run them end to end). The **Phase 2 review** needs
all stages complete.

## 2. Implementation Principles

1.  Freeze the common input, parsing, batching, query, and output
    contracts before implementing all adapters.
2.  Build and test one backend end-to-end before duplicating the
    implementation pattern across the remaining backends.
3.  Keep production parsing, batching, transformation, and analytics
    inside the selected backend.
4.  Keep controller responsibilities limited to orchestration, timing,
    output validation, and persistence.
5.  Validate correctness before comparing runtime.
6.  Keep generated data, run outputs, credentials, and
    environment-specific files out of Git.
7.  Record implementation decisions and test evidence as the project
    progresses.

## 3. Stage 1 - Repository and Configuration

### Tasks

-   [ ] Confirm the agreed repository directory structure.
-   [ ] Verify `.gitignore` excludes raw datasets, generated working
    data, run outputs, logs, caches, local environments, and secrets.
-   [ ] Verify `config/project_config.json` is valid JSON.
-   [ ] Confirm configured input paths and July-before-August order.
-   [ ] Confirm the default batch size is 1,000,000.
-   [ ] Confirm adapter names and directories.
-   [ ] Confirm the common JSONL output filenames.
-   [ ] Confirm no credentials are committed.

### Completion gate

The repository structure and configuration agree with the architecture,
batching, parsing, and database documents.

## 4. Stage 2 - Common Contracts and Test Fixtures

### Tasks

-   [ ] Review `PARSING_RULES.md`.
-   [ ] Review `PARSING_TEST_CASES.md`.
-   [ ] Review `BATCHING_STRATEGY.md`.
-   [ ] Review `COMMON_DATA_MODEL.md`.
-   [ ] Create small deterministic input fixtures covering valid
    records, malformed records, missing request fields, optional bytes,
    CRLF, final unterminated lines, and batch boundaries.
-   [ ] Define expected parsed records and expected query outputs for
    each fixture.
-   [ ] Verify the same fixtures can be supplied to all four adapters.

### Completion gate

Expected outcomes are explicit and reproducible. Do not claim
cross-backend validation until each adapter has actually passed the
fixtures.

## 5. Stage 3 - PostgreSQL Schema and Persistence

### Tasks

-   [ ] Implement database creation/setup instructions.
-   [ ] Create the `runs` table.
-   [ ] Create the `run_batches` table.
-   [ ] Create the `phase_timings` table if phase-level diagnostics are
    retained.
-   [ ] Create the `malformed_samples` table.
-   [ ] Create the Q1, Q2, and Q3 result tables.
-   [ ] Add primary keys, foreign keys, checks, and indexes.
-   [ ] Add `UNIQUE NULLS NOT DISTINCT` constraints so per-batch and
    whole-run rows are both protected from duplicates.
-   [ ] Add `pipeline_name` and `executed_at` to every result table, and
    `rank` to Q2.
-   [ ] Implement transactional result loading.
-   [ ] Implement rollback and failure-status handling.
-   [ ] Implement elapsed-time persistence after result commit.
-   [ ] Test duplicate prevention and failed-load rollback.

### Completion gate

A controlled test run can be loaded atomically. A forced load failure
leaves no partially published query results.

## 6. Stage 4 - Backend Adapter Interface

### Tasks

-   [ ] Define a common adapter invocation mechanism in the controller.
-   [ ] Require `--batch-size N`.
-   [ ] Require `--output-dir DIR`.
-   [ ] Support repeated `--input FILE` arguments.
-   [ ] Preserve configured input order.
-   [ ] Launch adapters from the repository root.
-   [ ] Use a fresh output directory for every run.
-   [ ] Capture exit status and diagnostic output.
-   [ ] Reject missing or malformed output files.

### Completion gate

The controller can invoke a test adapter using the shared argument
contract and correctly detect success and failure.

## 7. Stage 5 - Implement the Four Pipelines

Implement each backend independently. The selected backend must perform
its own input reading, record numbering, batch assignment, parsing,
transformations, and query aggregations.

Follow decisions D1 and D2 (section 15) in this
stage, and read the backend warnings in `PARSING_RULES.md` section 11.

### 7.1 Apache Pig

-   [ ] Implement input reading and record handling.
-   [ ] Implement continuous record numbering and batch assignment.
-   [ ] Implement common parsing and malformed-record classification.
-   [ ] Implement Q1, Q2, and Q3.
-   [ ] Produce all five required JSONL output files.
-   [ ] Run shared fixtures and compare expected results.

### 7.2 Hadoop MapReduce

-   [ ] Implement input reading and record handling.
-   [ ] Implement continuous record numbering and batch assignment.
-   [ ] Implement common parsing and malformed-record classification.
-   [ ] Implement Q1, Q2, and Q3 using MapReduce jobs.
-   [ ] Produce all five required JSONL output files.
-   [ ] Run shared fixtures and compare expected results.

### 7.3 MongoDB

-   [ ] Implement input reading and record handling.
-   [ ] Implement continuous record numbering and batch assignment.
-   [ ] Load/process records using the agreed MongoDB design.
-   [ ] Implement common parsing and malformed-record classification.
-   [ ] Implement Q1, Q2, and Q3 using MongoDB processing/aggregation.
-   [ ] Produce all five required JSONL output files.
-   [ ] Run shared fixtures and compare expected results.

### 7.4 Apache Hive

-   [ ] Implement input reading and record handling.
-   [ ] Implement continuous record numbering and batch assignment.
-   [ ] Implement common parsing and malformed-record classification.
-   [ ] Implement Q1, Q2, and Q3 using Hive.
-   [ ] Produce all five required JSONL output files.
-   [ ] Run shared fixtures and compare expected results.

### Completion gate

Each adapter independently passes the shared parsing, batching, and
query fixtures. Do not substitute Python-side ETL for backend
processing.

## 8. Stage 6 - Python Controller

### Tasks

-   [ ] Validate selected pipeline and positive batch size.
-   [ ] Generate UUID run IDs.
-   [ ] Record UTC start timestamps.
-   [ ] Create fresh run-specific output directories.
-   [ ] Start timing just before launching the adapter, and insert the
    run row as `RUNNING`.
-   [ ] Invoke exactly one selected adapter.
-   [ ] Validate adapter exit status and output files.
-   [ ] Load run metadata, batches, samples, and query rows
    transactionally.
-   [ ] Stop timing after result transaction commit.
-   [ ] Persist elapsed seconds.
-   [ ] Mark the run complete only after elapsed time is saved.
-   [ ] Mark failures as `FAILED` with an error message, without
    publishing partial query results.
-   [ ] Implement failure handling and abandoned-run recovery as
    specified in `DATABASE_DESIGN.md` section 12. Recovery must never
    touch a run that another controller is still executing.

### Completion gate

End-to-end controller tests demonstrate correct success, adapter
failure, output validation failure, database rollback, and elapsed-time
persistence behaviour.

## 9. Stage 7 - Streamlit Frontend and Reporting

### Tasks

-   [ ] Add pipeline selection.
-   [ ] Add batch-size configuration.
-   [ ] Add run initiation.
-   [ ] Display run status and failure details.
-   [ ] Display runtime and batch statistics.
-   [ ] Display malformed-record counts and retained samples.
-   [ ] Display Q1 daily traffic results.
-   [ ] Display Q2 top resources.
-   [ ] Display Q3 hourly error analysis.
-   [ ] Distinguish per-batch results from whole-run results.
-   [ ] Display NULL batch IDs as `Whole run`.
-   [ ] Add run history and pipeline comparisons.
-   [ ] Keep report rendering outside the measured ETL runtime.

### Completion gate

The user can launch a selected pipeline and inspect its committed
results and run metadata through Streamlit.

## 10. Stage 8 - Full-Dataset Correctness Validation

### Tasks

-   [ ] Verify original input filenames, order, sizes, and checksums.
-   [ ] Run each backend on the complete dataset.
-   [ ] Compare total input records and batch counts.
-   [ ] Compare malformed counts.
-   [ ] Compare Q1 per-batch and whole-run results.
-   [ ] Compare Q2 per-batch and whole-run results.
-   [ ] Verify global distinct-host counts.
-   [ ] Verify top-20 ordering and tie-breaking.
-   [ ] Compare Q3 per-batch and whole-run results.
-   [ ] Compare error rates within absolute tolerance `1e-9`.
-   [ ] Investigate and document every discrepancy.

### Completion gate

All four backends produce equivalent results under the common contract,
or any unresolved differences are explicitly documented and excluded
from performance claims.

## 11. Stage 9 - Benchmark Experiments

Use the batch sizes specified in `EXPERIMENT_DESIGN.md`:

-   100,000 records.
-   500,000 records.
-   1,000,000 records.

### Tasks

-   [ ] Confirm correctness before timing.
-   [ ] Run at least three trials for every pipeline and batch-size
    combination.
-   [ ] Record run IDs and environment details.
-   [ ] Retain failed trial records and explain exclusions.
-   [ ] Calculate median runtime from successful trials.
-   [ ] Compare runtime across pipelines and batch sizes.
-   [ ] Prepare tables and charts.
-   [ ] Explain observed results without claiming universal technology
    rankings.

### Completion gate

The benchmark report is reproducible and includes correctness status,
trial counts, median runtimes, and experimental limitations.

## 12. Stage 10 - Final Report and Submission

### Tasks

-   [ ] Update the report with the final architecture.
-   [ ] Explain each backend implementation.
-   [ ] Include database schema and application workflow.
-   [ ] Include parsing and correctness validation evidence.
-   [ ] Include benchmark methodology and measured results.
-   [ ] Include runtime and result comparison charts.
-   [ ] Document limitations and implementation challenges.
-   [ ] Verify repository README and documentation.
-   [ ] Remove accidental generated files and secrets from the
    submission.
-   [ ] Confirm all required source files are committed.
-   [ ] Verify the final GitHub repository state.

### Completion gate

The report, source code, documentation, and submitted repository agree
with the implementation that was actually tested.

## 13. Suggested Team Workstreams

The following are functional workstreams, not a claim about current
ownership:

  -----------------------------------------------------------------------
  Workstream                          Main responsibility
  ----------------------------------- -----------------------------------
  MongoDB and application             MongoDB adapter and frontend
                                      integration

  Hive and database                   Hive adapter and shared PostgreSQL
                                      schema

  MapReduce                           Hadoop MapReduce adapter

  Pig and integration/reporting       Pig adapter and
                                      integration/reporting support
  -----------------------------------------------------------------------

Coordinate the shared contracts before parallel implementation. Each
workstream must use the same fixtures, schemas, query definitions, and
adapter interface.

## 14. Progress Tracking Rules

-   Mark a task complete only when its implementation exists.
-   Record the command or test used to validate important behaviour.
-   Distinguish planned, implemented, tested, and benchmarked work.
-   Do not describe placeholder directories as completed components.
-   Update this roadmap when scope or agreed design decisions change.

## 15. Open Design Decisions

Decisions are recorded here. D1 and D2 are decided; D3 and D4 are still open.

### D1 - How each backend assigns continuous record numbers (decided)

Every adapter uses the shared staging stage in `etl_common/staging.py`.
It reads the original files, numbers records and assigns batch IDs, and
passes unchanged record text to the engine. See `BATCHING_STRATEGY.md`
section 2.

### D2 - What "processing in batches" means for each backend (decided)

Each engine parses and computes per-batch results one batch at a time,
then computes whole-run results from all parsed records. See
`BATCHING_STRATEGY.md` section 11 for the concrete steps per engine.

### D3 - NULL batch ID for whole-run rows

The brief asks for a batch identifier on stored results. Whole-run rows
use `batch_id = NULL` (shown as "Whole run"). Keep this, but explain it
in the report and at the Phase 1 review, or confirm it with the
instructor.

### D4 - Adapter launch commands

`config/project_config.json` has a `launcher` field for each adapter,
currently `null`. Fill in the exact command for each backend (for
example a shell script inside its pipeline directory). The controller
must refuse to run an adapter whose launcher is not set.
