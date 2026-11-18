# Batching Strategy

## DAS 839 - NoSQL Systems End Semester Project

## 1. Default Configuration

Default batch size: 1,000,000 input records.

Input files must be processed in this exact order:

1. `NASA_access_log_Jul95`
2. `NASA_access_log_Aug95`

The configured batch size is the maximum number of original input records in a batch.

## 2. Batch Ownership and Record Numbering (decision D1)

The selected backend adapter is responsible for:

- Reading the original input files.
- Assigning continuous record numbers.
- Assigning batch IDs.
- Processing records batch by batch (section 11).
- Producing batch-level analytics.
- Producing whole-run analytics.

The Python controller does not read, batch, parse or transform the input records. It passes the configured batch size and ordered original input paths to exactly one selected adapter.

### 2.1 Shared staging stage

Every adapter starts with the same **staging stage**, implemented once in `etl_common/staging.py`. It is the only code that reads the original files. It:

1. Reads the files in the given order, in binary mode.
2. Splits records on LF only and applies the line rules in section 3.
3. Decodes each record as ISO-8859-1.
4. Assigns the continuous record number and the batch ID.
5. Hands each **unchanged** record text, with its record number, batch ID and source file name, to the engine.

It never parses, cleans, filters or converts fields. Every engine therefore receives identical records, numbers and batch IDs. The stage runs inside the adapter, so it is included in the measured runtime for every pipeline.

Why a shared stage instead of engine-native numbering: Hadoop-based engines read input splits in parallel, so no task knows a line's global position. Engine-native numbering (Hive `ROW_NUMBER()` over block offsets, Pig `RANK`, a MapReduce line-count pre-pass) would add extra jobs that differ by engine. It would also depend on each engine's line reader, which by default also splits on a lone CR. One shared stage gives identical numbering and leaves all parsing and analytics in the engines.

### 2.2 How staged records reach each engine

| Engine | Staged form |
|---|---|
| MongoDB | One document per record in the run's `raw_lines` collection: `{_id: record_number, batch_id, source_file, line}`, inserted one batch at a time with `insert_many` |
| Hive, Pig, MapReduce | One UTF-8 text file per batch: `<work dir>/staging/batch_id=<N>/records.txt` |

Each staged text line is `record_number TAB source_file TAB text`, then LF. The text is the unchanged record and may contain tabs or CR. Readers therefore:

- split each staged line on the **first two tabs only**;
- end records at **LF only**. Set `textinputformat.record.delimiter` to `\n` so Hadoop's line reader does not also split on CR, and confirm with test T40.

The adapter's work directory is `<output-dir>/_work/`. It is deleted when the adapter succeeds, and the validator ignores any entry whose name starts with `_`.

`scripts/create_batches.py` uses the same line rules to write diagnostic batch files. It is not part of the production execution path.

## 3. Input Record Definition

One physical line represents one input record.

Input encoding: ISO-8859-1.

The two input files are read sequentially, July first and August second. Record numbering continues across the file boundary.

Line handling:

- LF terminates a record. A lone CR is ordinary content, not a terminator.
- For CRLF, remove the CR immediately preceding LF.
- Count a final unterminated line as a record.
- A trailing LF does not create an additional empty record.
- A blank physical line is still an input record and is treated as malformed by the parser.
- Do not otherwise modify the original record before backend parsing.
- If a file's last line has no LF, it is still a separate record. It must never be joined to the first line of the next file.

Hadoop's default line reader and Python's universal-newline mode do not follow these rules by default; see `PARSING_RULES.md` section 11.

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
- Batch ID.
- Malformation reason code (`PARSING_RULES.md` section 9).
- Original raw line.

The total malformed count must still include every malformed record, not only the retained samples.

## 9. Runtime Measurement

Runtime measurement is coordinated by the Python controller.

The controller cannot see when an adapter begins reading, so it starts the timer just before it launches the adapter. Backend startup (JVM, Hadoop, database connections) is therefore included, in the same way for every pipeline.

It includes:

- Backend startup.
- Reading the original input files.
- Batch assignment.
- Parsing and transformation.
- Query execution and aggregation.
- Writing adapter outputs.
- Output validation.
- Loading results into PostgreSQL.
- Committing the run and its results.

The timer stops after the final result transaction commits successfully. The elapsed time is then saved and the run is marked `COMPLETED` (see `DATABASE_DESIGN.md` section 12).

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

## 11. Per-Engine Batch Processing (decision D2)

All four engines follow the same algorithm, so batch size affects each of them in an equivalent way:

1. **Stage** the records (section 2).
2. **For each batch k = 1..n, in order:**
   1. Make batch k available to the engine.
   2. Parse and classify batch k's records **in the engine** into the run's parsed store. Each parsed record keeps its record number, batch ID, source file, validity, reason code and fields.
   3. Compute batch k's record count, malformed count, and per-batch Q1, Q2 and Q3, from batch k's parsed records only.
3. **After the last batch:** compute whole-run Q1, Q2 and Q3 from **all parsed records**. Never merge per-batch aggregates; distinct-host counts and the top 20 cannot be merged correctly.
4. **Malformed samples:** take the 100 malformed records with the lowest record numbers.
5. **Export** the results and convert them to the JSONL output files. Converting serialization is allowed; changing values is not.

Every record is parsed exactly once, during its batch.

| Engine | Step 2.1: make batch k available | Step 2.2: parse | Step 2.3: per-batch results | Step 3: whole run |
|---|---|---|---|---|
| **MongoDB** | `insert_many` batch k's documents into `raw_lines` | Aggregation on `raw_lines` filtered by `batch_id = k`: `$regexFind` with the section 5 pattern of `PARSING_RULES.md`, validation with `$switch`/`$cond`, then `$merge` into `parsed` | Aggregations on `parsed` with `{batch_id: k, valid: true}`: `$group` for Q1 and Q3; for Q2, `$group` by (path, host), then by path, then `$sort` and `$limit: 20` | The same aggregations on `parsed` without the batch filter, with `allowDiskUse: true` |
| **Hive** | `ALTER TABLE raw_staged ADD PARTITION (batch_id=k)` pointing at the batch directory. `raw_staged` uses `RegexSerDe` `^([0-9]+)\t([^\t]*)\t(.*)$` | `INSERT INTO parsed PARTITION (batch_id=k) SELECT ...` using `regexp_extract` and `CASE` checks. No `unix_timestamp`, `from_unixtime` or `to_date` | `INSERT OVERWRITE DIRECTORY` queries over `parsed WHERE batch_id = k` | The same queries over all of `parsed` |
| **Pig** | Run `batch.pig` with `-param BATCH=k` on the batch directory, loaded with `TextLoader` | `REGEX_EXTRACT_ALL` with the section 5 pattern and `bincond` checks; `STORE` to `parsed/batch_id=k` | In the same `batch.pig` run, `GROUP`/`FOREACH` for Q1 to Q3 on batch k; `ORDER ... LIMIT 20` for Q2 | `whole_run.pig` loads `parsed/*` |
| **MapReduce** | Use batch k's directory as job input | Map-only parse job writing `parsed/batch_id=k`; malformed counts per reason as Hadoop counters | Jobs over `parsed/batch_id=k`: Q1; Q3; Q2 as (path, host) dedup, then path aggregation, then a single-reducer top 20 | The same jobs over `parsed/*` |

Rules that apply to every engine:

- Per-batch step 2.3 must not read other batches' parsed records.
- Whole-run step 3 must read parsed records, not per-batch results.
- The run's parsed store and staging data are created fresh for each run and removed when the adapter succeeds.
- Each adapter may report the duration of the staging, per-batch and whole-run steps in `phase_timings.jsonl`.
