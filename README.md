# Multi-Pipeline ETL and Reporting Framework

### DAS 839 - NoSQL Systems End Semester Project

## Project Overview

This project implements a comparative ETL and reporting
framework for NASA HTTP Web Server Logs.

The same dataset and analytical workload will be processed
using four different execution technologies:

1. Apache Pig
2. Hadoop MapReduce
3. MongoDB
4. Apache Hive

The objective is to compare their implementation approaches,
execution runtime, batching behaviour and suitability for
semi-structured log analytics.

## Architecture

The proposed application consists of:

- Python central controller
- Streamlit frontend
- Four independent ETL pipelines
- PostgreSQL reporting database
- Reporting module

The user selects exactly one execution pipeline per run.

## Dataset

Official NASA HTTP Web Server Logs:

- July 1995
- August 1995

Download:
https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

Place the downloaded and decompressed files in:

data/raw/

Required filenames:

- NASA_access_log_Jul95
- NASA_access_log_Aug95

## Mandatory Analytical Queries

### Q1: Daily Traffic Summary
Requests and bytes grouped by date and status code.

### Q2: Top Requested Resources
Top 20 resources by request count, with total bytes
and distinct host count.

### Q3: Hourly Error Analysis
Error requests, total requests, error rate and distinct
error hosts grouped by date and hour.

## Batching Strategy

Default batch size: 1,000,000 records.

The central controller partitions unchanged raw records
into sequential batches.

The selected execution engine performs all parsing,
transformation and aggregation.

## Technology Stack

| Component | Technology |
|---|---|
| Controller | Python |
| Frontend | Streamlit |
| Database | PostgreSQL |
| Pipeline 1 | Apache Pig |
| Pipeline 2 | Hadoop MapReduce |
| Pipeline 3 | MongoDB |
| Pipeline 4 | Apache Hive |

## Current Development Status

### Phase 1 - Architecture and Prototype

- [x] Dataset acquisition
- [x] Project structure
- [x] Common data model
- [x] ETL architecture design
- [x] Batching strategy
- [x] Raw batch manager
- [ ] Parsing implementation
- [ ] PostgreSQL schema
- [ ] First two working pipelines
- [ ] Frontend and reporting

## Documentation

Refer to the docs/ directory for:

- COMMON_DATA_MODEL.md
- ETL_ARCHITECTURE.md
- BATCHING_STRATEGY.md
