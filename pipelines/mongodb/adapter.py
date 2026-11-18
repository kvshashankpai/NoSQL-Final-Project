"""MongoDB adapter. Implements the shared adapter contract (docs/ETL_ARCHITECTURE.md section 5).

    python -m pipelines.mongodb.adapter --batch-size N --output-dir DIR --input FILE [--input FILE ...]
                                        [--debug-parsed] [--keep-work]
    python -m pipelines.mongodb.adapter --engine-info

Steps (docs/BATCHING_STRATEGY.md section 11):
1. Shared staging stage: number records and assign batches (etl_common.staging).
2. For each batch: insert its unchanged records into raw_lines; parse them in
   MongoDB into parsed; compute the batch's statistics and Q1-Q3 in MongoDB.
3. Whole run: Q1-Q3 over all parsed records, in MongoDB.
4. Malformed samples in MongoDB; export everything to JSONL.

Each run uses its own temporary database, dropped at the end unless --keep-work.
Connection: MONGODB_URI (default mongodb://localhost:27017).
"""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

from etl_common.config import load_dotenv
from etl_common.staging import iter_records
from pipelines.mongodb import pipeline as P

INSERT_CHUNK = 10_000


def connect():
    from pymongo import MongoClient
    load_dotenv()
    uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def engine_info():
    import pymongo
    client = connect()
    try:
        info = client.server_info()
        return {"engine": "MongoDB", "server_version": info.get("version"),
                "driver": f"pymongo {pymongo.version}", "mode": "standalone"}
    finally:
        client.close()


class Timer:
    def __init__(self):
        self.totals = {}

    def add(self, name, seconds):
        self.totals[name] = self.totals.get(name, 0.0) + seconds


def _write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _agg(coll, stages):
    return list(coll.aggregate(stages, allowDiskUse=True))


def _scoped(rows, batch_id):
    for r in rows:
        yield {"batch_id": batch_id, **r}


def run(input_files, batch_size, output_dir, debug_parsed=False, keep_work=False):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(not p.name.startswith("_") for p in output_dir.iterdir()):
        raise SystemExit(f"output directory is not empty: {output_dir}")

    timer = Timer()
    client = connect()
    db_name = f"nasa_etl_{uuid.uuid4().hex[:12]}"
    db = client[db_name]
    raw, parsed = db["raw_lines"], db["parsed"]
    raw.create_index("batch_id")
    parsed.create_index([("batch_id", 1), ("valid", 1)])

    batches, daily, top, hourly = [], [], [], []

    def process_batch(k):
        t = time.perf_counter()
        raw.aggregate(P.parse_stages(k, "parsed"), allowDiskUse=True)
        timer.add("parse", time.perf_counter() - t)
        t = time.perf_counter()
        (stats,) = _agg(parsed, P.batch_stats_pipeline(k))
        batches.append({"batch_id": k, "record_count": stats["record_count"],
                        "malformed_count": stats["malformed_count"]})
        daily.extend(_scoped(_agg(parsed, P.daily_traffic_pipeline(k)), k))
        top.extend(_scoped(_agg(parsed, P.top_resources_pipeline(k)), k))
        hourly.extend(_scoped(_agg(parsed, P.hourly_errors_pipeline(k)), k))
        timer.add("per_batch_queries", time.perf_counter() - t)

    try:
        # 1-2. Stage records and process each batch as soon as it is complete.
        current, chunk = None, []
        t = time.perf_counter()
        for rec in iter_records(input_files, batch_size):
            if current is not None and rec.batch_id != current:
                if chunk:
                    raw.insert_many(chunk, ordered=False)
                    chunk = []
                timer.add("staging", time.perf_counter() - t)
                process_batch(current)
                t = time.perf_counter()
            current = rec.batch_id
            chunk.append({"_id": rec.record_number, "batch_id": rec.batch_id,
                          "source_file": rec.source_file, "line": rec.text})
            if len(chunk) >= INSERT_CHUNK:
                raw.insert_many(chunk, ordered=False)
                chunk = []
        if chunk:
            raw.insert_many(chunk, ordered=False)
        timer.add("staging", time.perf_counter() - t)
        if current is not None:
            process_batch(current)

        # 3. Whole run, from all parsed records.
        t = time.perf_counter()
        daily.extend(_scoped(_agg(parsed, P.daily_traffic_pipeline(None)), None))
        top.extend(_scoped(_agg(parsed, P.top_resources_pipeline(None)), None))
        hourly.extend(_scoped(_agg(parsed, P.hourly_errors_pipeline(None)), None))
        samples = _agg(parsed, P.malformed_samples_pipeline())
        timer.add("whole_run_queries", time.perf_counter() - t)

        # 4. Export.
        t = time.perf_counter()
        _write_jsonl(output_dir / "batches.jsonl", batches)
        _write_jsonl(output_dir / "daily_traffic.jsonl", daily)
        _write_jsonl(output_dir / "top_resources.jsonl", top)
        _write_jsonl(output_dir / "hourly_errors.jsonl", hourly)
        _write_jsonl(output_dir / "malformed_samples.jsonl", samples)
        if debug_parsed:
            _write_jsonl(output_dir / "parsed_records.jsonl",
                         parsed.aggregate(P.parsed_records_pipeline(), allowDiskUse=True))
        timer.add("export", time.perf_counter() - t)
        _write_jsonl(output_dir / "phase_timings.jsonl",
                     [{"phase_name": k, "duration_seconds": v} for k, v in timer.totals.items()])
    finally:
        if not keep_work:
            client.drop_database(db_name)
        client.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="MongoDB ETL adapter")
    ap.add_argument("--batch-size", type=int)
    ap.add_argument("--output-dir", type=Path)
    ap.add_argument("--input", action="append", type=Path, default=[])
    ap.add_argument("--debug-parsed", action="store_true")
    ap.add_argument("--keep-work", action="store_true", help="keep the temporary MongoDB database")
    ap.add_argument("--engine-info", action="store_true", help="print engine versions as JSON and exit")
    args = ap.parse_args(argv)
    if args.engine_info:
        print(json.dumps(engine_info()))
        return 0
    if not args.batch_size or args.batch_size <= 0 or not args.output_dir or not args.input:
        ap.error("--batch-size (positive), --output-dir and at least one --input are required")
    run(args.input, args.batch_size, args.output_dir, args.debug_parsed, args.keep_work)
    return 0


if __name__ == "__main__":
    sys.exit(main())
