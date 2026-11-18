# Hadoop MapReduce pipeline

This directory contains the Phase 1 MapReduce implementation for the NASA HTTP log project. The adapter accepts the shared pipeline arguments, stages input with the project-wide batching rules, runs Java Hadoop jobs, writes the common JSONL output contract, and leaves PostgreSQL loading to the controller.

For overall project setup, see the [root README](../../README.md). This guide covers the MapReduce path in more detail.

## Prerequisites

- Python 3 and the project requirements installed in the active virtual environment.
- Hadoop 3.x on `PATH`, including the `hadoop` executable and `hadoop classpath` command.
- Java/JDK compatible with the installed Hadoop distribution.
- The bundled `build/mapreduce-parser.jar` (already included in the project).
- For normal controller runs, PostgreSQL 15 or later and `.env` configured as described below.

Check the tools with:

```bash
python3 --version
hadoop version
javac -version
```

## Input and batch handling

By default, the controller reads these files in this order:

1. `data/raw/NASA_access_log_Jul95`
2. `data/raw/NASA_access_log_Aug95`

The logs are read as ISO-8859-1. Every physical LF-terminated line is a record; CRLF and unterminated final records are handled by `etl_common/staging.py`. Record numbering continues across both files. Batch IDs start at 1, and the final partial batch is included.

The adapter calls the shared `write_staged_batches` function, which writes `records.txt` under `batch_id=N` staging directories. Embedded CR characters are protected before Hadoop reads a batch and restored by the Java parser, so Hadoop's record reader does not split a record at a CR.

## Execution flow

1. `controller/main.py` selects the pipeline and batch size, creates a run ID and a fresh output directory, and records the `RUNNING` database row for persisted runs.
2. `pipelines/mapreduce/adapter.py` stages the ordered inputs into batches.
3. For each batch, the adapter runs the Java parser and the Q1 daily traffic, Q2 resource aggregation, and Q3 hourly error jobs through `hadoop jar` in local mode.
4. The adapter assembles batch and whole-run rows. Global Q2 is aggregated again across all parsed records before ranking, so distinct hosts are deduplicated across batches. The whole-run Q1 and Q3 rows are assembled by the adapter from the batch/parser results.
5. The adapter writes the common JSONL files. The controller validates their schemas and consistency before it loads any results.
6. With database loading enabled, the controller stores batches, malformed samples, phase timings, Q1–Q3 rows, and run metadata in PostgreSQL. Result rows are loaded transactionally, and the run is marked `COMPLETED` after the results commit.

The Java source is under `src/main/java/nosql/mapreduce/`. The principal classes are:

| Work | Classes |
|---|---|
| Parsing | `LogParser`, `ParseLogsDriver`, `ParseLogsMapper`, `ParsedRecord` |
| Q1 daily traffic | `q1/DailyTrafficDriver`, `DailyTrafficMapper`, `DailyTrafficReducer` |
| Q2 per-batch/global resources | `q2/ResourceDriver`, `ResourceMapper`, `ResourceReducer`, `TopResourcesDriver`, `TopResourcesMapper`, `TopResourcesReducer` |
| Q3 hourly errors | `q3/HourlyErrorDriver`, `HourlyErrorMapper`, `HourlyErrorReducer` |

## Run MapReduce

From the repository root, activate the project environment. Start the interactive selector with:

```bash
python3 controller/main.py
```

Select **1. MapReduce**, then choose 100,000, 500,000, 1,000,000, or a custom batch size. The CLI form is useful for reproducible runs:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 100000
python3 controller/main.py --pipeline mapreduce --batch-size 500000
python3 controller/main.py --pipeline mapreduce --batch-size 1000000
```

To override inputs, repeat `--input` in the required order:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 1000000 \
  --input data/raw/NASA_access_log_Jul95 \
  --input data/raw/NASA_access_log_Aug95
```

For local processing and output validation without a database connection, add `--no-db`:

```bash
python3 controller/main.py --pipeline mapreduce --batch-size 1000000 --no-db
```

To call the adapter directly (it does not write to PostgreSQL), pass a fresh output directory:

```bash
python3 pipelines/mapreduce/adapter.py \
  --batch-size 1000000 \
  --output-dir results/mapreduce-manual-run \
  --input data/raw/NASA_access_log_Jul95 \
  --input data/raw/NASA_access_log_Aug95
```

Do not reuse an existing output directory. For debugging, add `--debug-parsed` to produce `parsed_records.jsonl`.

## PostgreSQL setup

The project Compose service publishes PostgreSQL on host port 5433 to avoid conflicts with a local server on 5432. Ensure `.env` contains matching settings (`PGHOST=localhost`, `PGPORT=5433`, `PGUSER=etl`, `PGDATABASE=nosql_final_project`, and a `PGPASSWORD` used by Compose).

```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U etl -d nosql_final_project \
  -v ON_ERROR_STOP=1 < database/schema.sql
```

Normal controller runs validate adapter output before storing it. The adapter itself never connects to the database.

## Output files

Every run directory contains:

| File | Contents |
|---|---|
| `batches.jsonl` | Record and malformed counts for each non-empty batch |
| `daily_traffic.jsonl` | Q1 per-batch and whole-run results |
| `top_resources.jsonl` | Q2 ranked per-batch and whole-run results |
| `hourly_errors.jsonl` | Q3 per-batch and whole-run results |
| `malformed_samples.jsonl` | Up to the first 100 malformed input examples |
| `phase_timings.jsonl` | Adapter phase timing diagnostics |
| `parsed_records.jsonl` | Optional parsed-record diagnostics (`--debug-parsed`) |

For whole-run analytics, `batch_id` is JSON `null`; per-batch rows have positive IDs. Run directories are created at `results/<run-id>/` by default. The controller prints the run ID, total records, elapsed time, and output directory on success.

## Validate a run

The controller validates outputs automatically before a database load. To run the existing MapReduce fixture comparisons:

```bash
python3 -m controller.fixture_runner --pipeline MapReduce
```

The fixture runner checks parsing boundaries, malformed records, query output, Top-20 ordering, and JSONL contract equivalence. The actual NASA files are much larger; run one full-data execution only after the small fixture checks pass.

## Troubleshooting

- **`psycopg` missing:** activate `.venv` and install dependencies with `python -m pip install -r requirements.txt`.
- **Docker daemon unavailable:** open Docker Desktop, wait until it is ready, then run `docker compose up -d postgres`.
- **Port 5433 cannot bind:** another process is already using 5433. Check it before changing the Compose host port and the matching `PGPORT` in `.env`.
- **Role `etl` does not exist:** confirm the controller is connecting to port 5433 and the Compose service is running; port 5432 may be a different local PostgreSQL instance.
- **`hadoop: command not found`:** install/configure Hadoop and add its `bin` directory to `PATH`.
- **Input file not found:** place the decompressed official files under `data/raw/` with the configured filenames, or provide repeated `--input` arguments.
- **Output directory already exists:** select a new output path or let the controller create a fresh run-specific directory.
- **Hadoop job failure:** check the error output for the Java driver and confirm the bundled JAR exists at `pipelines/mapreduce/build/mapreduce-parser.jar`.
