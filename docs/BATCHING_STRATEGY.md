# Batching Strategy
## DAS 839 - NoSQL Systems End Semester Project

## 1. Default Configuration

Default batch size: 1,000,000 input records.

Input order:
1. NASA_access_log_Jul95
2. NASA_access_log_Aug95

## 2. Batch Creation

The central Python controller reads the original files
sequentially and divides them into batches.

Each batch contains at most the configured number
of original log records.

The final batch may contain fewer records.

Batch IDs begin at 1 and increase sequentially.

## 3. Raw Data Preservation

The batch manager:

- Does not parse records.
- Does not clean records.
- Does not filter records.
- Does not convert records to another format.
- Does not modify whitespace or line endings.

Original input bytes are copied unchanged.

The selected ETL engine performs all actual
parsing and transformations.

## 4. Batch Manifest

Each run has a manifest.json file containing:

- Run identifier
- Batch size
- Total input records
- Number of batches
- Average batch size
- Individual batch identifiers
- Batch record counts
- Source file segments

## 5. Average Batch Size

Average batch size is:

Total records processed / Number of non-empty batches

The final partial batch is included.

## 6. Global Aggregation

Batch-level intermediate results must be combined
inside the selected processing engine.

Q1:
Sum request counts and byte totals for each
date and status code.

Q2:
Combine resource counts and byte totals globally.
Calculate distinct hosts across the complete dataset.
Select the global top 20 resources.

Q3:
Combine total request counts and error counts globally.
Calculate distinct error hosts across the complete
dataset for each date and hour.

Distinct-host counts must not be added directly
across batches because hosts may occur in multiple
batches.

## 7. Runtime

Runtime includes:

- Reading original input files
- Creating raw batches
- Executing the selected ETL pipeline
- Global aggregation
- Loading results into PostgreSQL

Report rendering is excluded.

## 8. Temporary Data

Batch files are stored in:

data/work/<run_id>/

Each execution receives its own directory.
Original NASA files are never modified.
