# ETL Architecture
## DAS 839 - NoSQL Systems End Semester Project

## 1. Objective

Build a multi-pipeline ETL and reporting framework for
NASA HTTP Web Server Logs.

The same source dataset and analytical workload will be
executed using four independent processing technologies:

1. Apache Pig
2. Hadoop MapReduce
3. MongoDB
4. Apache Hive

## 2. System Architecture

The application consists of:

- Streamlit frontend
- Python central controller
- Raw batch manager
- Four independent ETL execution pipelines
- PostgreSQL reporting database
- Reporting module

The user selects exactly one execution pipeline for each run.

## 3. Input Dataset

Official NASA HTTP logs:

- NASA_access_log_Jul95
- NASA_access_log_Aug95

The original files are retained unchanged in data/raw/.

## 4. ETL Workflow

### Extract
Read the original NASA log files in their specified order.

### Batch
The Python controller divides original input records into
fixed-size batches without modifying their contents.

### Transform
The selected execution engine performs:

- Log record parsing
- Timestamp extraction
- Date and hour derivation
- HTTP request decomposition
- Missing byte handling
- Malformed-record identification

### Analytics
Each pipeline executes the same three queries:

- Q1: Daily Traffic Summary
- Q2: Top Requested Resources
- Q3: Hourly Error Analysis

### Load
Aggregated results and execution metadata are written
to PostgreSQL.

### Report
Streamlit reads PostgreSQL and displays results,
runtime and batching statistics.

## 5. Technology Responsibilities

Python:
- User interface orchestration
- Pipeline selection
- Raw batching
- Runtime measurement
- Database coordination

Pig:
- Pig-based ETL and aggregation

MapReduce:
- Java Hadoop Mapper and Reducer processing

MongoDB:
- MongoDB document processing and aggregation

Hive:
- Hive SQL-based ETL and aggregation

PostgreSQL:
- Common reporting and metadata storage

Streamlit:
- User interface and reporting

## 6. Equivalence Requirements

All four pipelines must preserve:

- Identical source records
- Identical parsing rules
- Identical cleaning rules
- Identical query definitions
- Identical grouping and aggregation semantics
- Identical output schemas

The processing logic must execute within the selected
technology. Python must not substitute for the four
execution engines.

## 7. Runtime Measurement

Runtime begins before the original input files are read.

It ends after all final aggregated results and execution
metadata have been successfully committed to PostgreSQL.

Dataset download, installation and report rendering
are excluded.
