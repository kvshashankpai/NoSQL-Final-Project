# Multi-Pipeline ETL and Reporting Framework

## DAS 839 — NoSQL Systems End-Semester Project

A comparative ETL and reporting framework for the NASA HTTP Web Server Logs (July and August 1995). The same input data, parsing rules, batching semantics, and analytical workload are implemented independently using Apache Pig, Hadoop MapReduce, MongoDB, and Apache Hive.

The application provides a common execution interface through a Python controller and Streamlit frontend, with PostgreSQL used to persist run metadata and query results.

## 1. Project Objectives

- Implement the same ETL workload in four different data-processing technologies.
- Compare correctness, execution time, and batching behaviour under consistent conditions.
- Preserve backend independence: the selected backend performs its own input reading, parsing, transformation, batching, and analytics.
- Store results in a common PostgreSQL schema for reporting and comparison.
- Provide a Streamlit interface for selecting a pipeline, launching runs, and inspecting results.

## 2. Technology Stack

| Component | Technology | Responsibility |
|---|---|---|
| Frontend | Streamlit | Pipeline selection, run controls, results and comparisons |
| Controller | Python | Orchestration, timing, adapter invocation and database loading |
| Pipeline 1 | Apache Pig | ETL and analytics in Pig |
| Pipeline 2 | Hadoop MapReduce | ETL and analytics in Java MapReduce |
| Pipeline 3 | MongoDB | Document processing and aggregation |
| Pipeline 4 | Apache Hive | ETL and analytics using Hive |
| Reporting database | PostgreSQL | Common run metadata and query-result storage |

## 3. Dataset

**Source:** NASA HTTP Web Server Logs.

Place the downloaded and decompressed files in `data/raw/` using these exact filenames:

```text
data/raw/NASA_access_log_Jul95
data/raw/NASA_access_log_Aug95
```

Process the files in this order:

1. `NASA_access_log_Jul95`
2. `NASA_access_log_Aug95`

Input encoding is ISO-8859-1. One physical line represents one input record. The original dataset files are not modified.

Dataset source: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

Raw datasets and generated working data are intentionally excluded from Git.

## 4. Architecture and Execution Model

The user selects exactly one pipeline for each run.

1. Streamlit collects the selected pipeline and batch size.
2. The Python controller validates the configuration and creates a UUID run ID.
3. The controller records the UTC start time and starts the runtime timer, then inserts the run row with status `RUNNING`. The controller cannot see when an adapter starts reading, so the timer starts before the adapter is launched and backend startup (JVM, Hadoop, database connections) is included for every pipeline.
4. The controller invokes exactly one backend adapter, passing the original input paths in order, the batch size, and a fresh output directory.
5. The selected adapter reads the source files, numbers records continuously, assigns batches, parses records, and executes Q1, Q2, and Q3 within its own backend technology.
6. The adapter writes the agreed JSONL output files.
7. The controller validates the adapter exit status and output files.
8. The controller loads batch metadata, malformed samples, and all query results into PostgreSQL in one transaction.
9. After the result transaction commits, the controller stops the timer.
10. The controller saves elapsed seconds and marks the run `COMPLETED`. Failure handling and abandoned-run recovery are specified in `docs/DATABASE_DESIGN.md` section 12.

The Python controller coordinates the work; it does not replace the ETL or analytics implementation of any backend.

### Phase 1 prototype (MapReduce)

The Phase 1 executable prototype is Hadoop MapReduce. The menu keeps all four planned pipeline choices visible, while the MapReduce launcher is the implemented production path at this stage. The other launchers remain unset until those adapters are built; choosing one reports that it is unavailable and does not create a database run.

From the repository root, start the interactive menu with:

```bash
python3 controller/main.py
```

For repeatable execution, use the CLI:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 1000000
```

The default input order comes from `config/project_config.json` and expects `data/raw/NASA_access_log_Jul95` followed by `data/raw/NASA_access_log_Aug95`. A custom batch size and explicit input list can be supplied without editing source:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 100000 --input data/raw/NASA_access_log_Jul95 --input data/raw/NASA_access_log_Aug95
```

To execute the synthetic parsing fixture through the controller and validate its JSONL outputs without PostgreSQL:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 10 --input tests/fixtures/parsing/input/cases.log --no-db --output-dir /tmp/nosql-mapreduce-smoke
```

Install Python requirements and ensure Hadoop 3.x (`hadoop` command) is on `PATH`. For database persistence, configure `.env` from `.env.example`, start PostgreSQL with `docker compose up -d postgres`, and apply the schema once:

```bash
python3 -m pip install -r requirements.txt
cp .env.example .env
docker compose up -d postgres
psql -h localhost -U etl -d nosql_final_project -v ON_ERROR_STOP=1 -f database/schema.sql
```

After placing both NASA source files in `data/raw/`, run the full configured dataset with:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 1000000
```

Omit `--pipeline` to select MapReduce interactively. Each run uses a fresh `results/<run-id>/` directory. `--no-db` is intended for local adapter development; normal runs validate first and then load their results to PostgreSQL.

### Adapter command-line contract

Each adapter must accept:

```text
--batch-size N
--output-dir DIR
--input FILE
```

Repeat `--input` for each source file, preserving configured order. Adapters are launched from the repository root, and relative paths are resolved from that root. Each run receives a fresh output directory.

The exact executable differs by backend; the argument contract is shared.

## 5. Batching Contract

- Default batch size: **1,000,000 input records**.
- Production batching is owned by the selected backend adapter.
- Record numbering starts at 1 and continues across the July and August files.
- Every input record receives a batch ID, including malformed records.
- The final partial batch is counted.
- A blank physical line is an input record and is malformed.
- A trailing LF does not create an extra record.
- A final unterminated line is counted.
- For CRLF, remove the CR immediately preceding LF; otherwise preserve the record content.

Batch assignment:

```text
batch_id = floor((record_number - 1) / batch_size) + 1
```

Every adapter assigns record numbers and batches with the shared staging stage in `etl_common/staging.py`, then processes one batch at a time in its engine (`docs/BATCHING_STRATEGY.md` sections 2 and 11). `scripts/create_batches.py` uses the same rules to write diagnostic batch files; it is not part of the production path.

Average batch size is total input records divided by the number of non-empty batches. It is NULL when there are no non-empty batches.

## 6. Common Parsing Rules

The expected record structure is:

```text
host ident authuser [timestamp] "request" status bytes
```

Key rules:

- Preserve host, timestamp, method, path, and protocol values; do not normalize case or URL-decode paths.
- Preserve the original logged clock time. Derive date and hour from that clock time without converting to UTC or machine-local time.
- Require a valid timestamp and a three-digit numeric status code.
- A missing bytes field or bytes value `-` becomes zero.
- A non-numeric or negative bytes value is malformed.
- A request of `-` or an empty request is valid, with request fields set to NULL.
- Split request contents on horizontal whitespace. The first three tokens map to method, resource path, and protocol.
- If more than three request tokens exist, retain the method and set path and protocol to NULL. This alone does not make the record malformed.
- Missing request components alone do not make a record malformed.
- Malformed records retain their record number and batch assignment, count toward malformed totals, and are excluded from Q1, Q2, and Q3.

Detailed rules and test cases are maintained in `docs/PARSING_RULES.md` and `docs/PARSING_TEST_CASES.md`.

## 7. Analytical Queries

All four pipelines implement the same three queries at both per-batch and whole-run levels.

### Q1 — Daily Traffic Summary

Group valid records by logged date and HTTP status code.

Calculate:

- Request count.
- Total bytes transferred.

All valid records are eligible, including records with missing request components.

### Q2 — Top Requested Resources

Include valid records whose resource path is not NULL. Group by the exact resource path.

Calculate:

- Request count.
- Total bytes transferred.
- Distinct requesting hosts.

Return the top 20 resources after aggregation. Sort by request count descending, then resource path ascending using binary UTF-8 ordering.

For whole-run results, distinct hosts must be deduplicated across the complete dataset. Do not add per-batch distinct-host counts.

### Q3 — Hourly Error Analysis

Group valid records by logged date and hour.

Calculate:

- Total request count.
- Error request count, where status is 400–599 inclusive.
- Distinct hosts among error requests.
- Error rate = error request count / total request count.

Include hours with valid requests and zero errors. Do not create rows for hours with no requests. Error rates are decimal fractions between 0 and 1; cross-pipeline comparisons use an absolute tolerance of `1e-9`.

## 8. Adapter Output Contract

Each adapter writes the following UTF-8 JSON Lines files into its run-specific output directory:

| File | Contents |
|---|---|
| `batches.jsonl` | One row per non-empty batch |
| `daily_traffic.jsonl` | Q1 per-batch and whole-run rows |
| `top_resources.jsonl` | Q2 per-batch and whole-run rows |
| `hourly_errors.jsonl` | Q3 per-batch and whole-run rows |
| `malformed_samples.jsonl` | First 100 malformed samples in input order |
| `phase_timings.jsonl` | Optional diagnostic phase durations |

Output requirements:

- One valid JSON object per line.
- Counts, status codes, hours, and byte totals are JSON numbers.
- Missing values are JSON `null`.
- Dates use `YYYY-MM-DD`.
- Per-batch analytics use a positive `batch_id`.
- Whole-run analytics use `"batch_id": null`.
- Empty results are represented by empty files.
- Column names and order follow the brief exactly; see `docs/COMMON_DATA_MODEL.md`.
- Top-resource rows include `rank` (1-20 within each scope).
- `batches.jsonl` contains `batch_id`, `record_count`, and `malformed_count`.
- Each malformed sample contains source file, source record number, batch ID, reason code, and raw line. Reason codes are fixed (see `docs/PARSING_RULES.md` section 9).
- The 100-sample limit does not limit malformed counting.

## 9. PostgreSQL Reporting Database

Database name: `nosql_final_project`.

The planned common tables are:

| Table | Purpose |
|---|---|
| `runs` | Pipeline, configuration, status, runtime and run-level counts |
| `run_batches` | Per-batch record and malformed counts |
| `phase_timings` | Optional diagnostic phase durations |
| `malformed_samples` | Retained malformed examples |
| `q1_daily_traffic` | Q1 results |
| `q2_top_resources` | Q2 results |
| `q3_hourly_errors` | Q3 results |

Every result table stores `run_id`, `pipeline_name`, `batch_id` and `executed_at`, as the brief requires. A NULL `batch_id` denotes a whole-run result; a positive value denotes a per-batch result. Do not use zero as a sentinel. Uniqueness uses `UNIQUE NULLS NOT DISTINCT` (PostgreSQL 15+), so duplicate whole-run rows are rejected.

The controller inserts each run as `RUNNING` first, so failed runs are kept with status `FAILED`. All result rows for a run are loaded in one transaction; on adapter or load failure, no partial query results are published. The authoritative runtime is stored in `runs.elapsed_seconds`.

See `docs/DATABASE_DESIGN.md` for the detailed schema and loading contract.

## 10. Repository Structure

```text
NoSQL-Final-Project/
├── README.md
├── .gitignore
├── config/
│   └── project_config.json
├── controller/
├── database/
├── data/
│   ├── raw/
│   ├── processed/
│   └── work/                 # Local generated working data; not committed
├── docs/
│   ├── BATCHING_STRATEGY.md
│   ├── COMMON_DATA_MODEL.md
│   ├── DATABASE_DESIGN.md
│   ├── ETL_ARCHITECTURE.md
│   ├── EXPERIMENT_DESIGN.md
│   ├── IMPLEMENTATION_ROADMAP.md
│   ├── PARSING_RULES.md
│   └── PARSING_TEST_CASES.md
├── frontend/
├── pipelines/
│   ├── mapreduce/
│   │   └── src/
│   └── pig/
├── reporting/
├── etl_common/               # Shared staging stage and config loading
├── tests/                    # Fixtures, oracle (test-only), unit and integration tests
├── verification/             # Input verification records (scripts/verify_inputs.py)
└── scripts/
    ├── create_batches.py     # Diagnostic utility only
    └── verify_inputs.py      # Input order, sizes and SHA-256
```

Directories are intended locations for project components. A placeholder directory does not imply that its implementation is complete.

## 11. Correctness and Benchmarking

Correctness must be established before runtime comparisons.

All four pipelines must be checked against the same parsing and batching test cases. Cross-pipeline validation should compare:

- Valid and malformed record classifications.
- Parsed field values.
- Record numbering and batch assignments.
- Per-batch query results.
- Whole-run query results.
- Distinct-host counts.
- Top-20 ordering.
- Error rates within absolute tolerance `1e-9`.

For benchmark comparisons, use the same input dataset, batch size, query definitions, and equivalent environment conditions. Record software versions and input checksums. Run at least three trials and report the median runtime.

Runtime begins when the controller starts the timer, just before it launches the adapter, and stops after the PostgreSQL result transaction commits. Persisting elapsed time follows the result commit. Dataset downloads, software installation, input checksum verification, and report rendering are excluded.

Do not state that a pipeline has been validated or benchmarked until its tests and runs have actually been completed.

## 12. Development Status

This README documents the agreed project design and interfaces. It does not claim that all pipeline implementations, database tables, frontend components, or cross-pipeline tests are complete.

Update this section as implementation progresses:

- [x] Executable test fixtures, output validator and comparator (`tests/`, `controller/validation.py`, `controller/compare.py`)
- [x] Shared staging stage for record numbering and batches (`etl_common/staging.py`)
- [x] Fixture runner for any adapter (`python -m controller.fixture_runner --pipeline <name>`)
- [x] PostgreSQL schema (`database/schema.sql`)
- [ ] Controller run lifecycle, transactional loader and recovery
- [ ] Report and Streamlit frontend with run history and comparison
- [ ] Apache Pig adapter
- [x] Phase 1 Hadoop MapReduce adapter and controller CLI (fixture validated)
- [ ] Cross-pipeline parsing and batching validation
- [ ] Full-dataset correctness validation
- [ ] Repeated benchmark trials

## 13. Documentation

Refer to the `docs/` directory for the authoritative project contracts:

- `ETL_ARCHITECTURE.md` — component responsibilities and execution workflow.
- `BATCHING_STRATEGY.md` — input record handling and batch assignment.
- `COMMON_DATA_MODEL.md` — shared logical fields and JSONL schemas.
- `PARSING_RULES.md` — parsing and malformed-record rules.
- `PARSING_TEST_CASES.md` — expected parsing and batching test outcomes.
- `DATABASE_DESIGN.md` — PostgreSQL tables, relationships, and transactional loading.
- `EXPERIMENT_DESIGN.md` — benchmark methodology and reproducibility.
- `IMPLEMENTATION_ROADMAP.md` — implementation stages, validation gates, and open design decisions.
- `ENVIRONMENT.md` — versions, connection settings, starting services, and adapter launch commands.
