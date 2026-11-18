#!/usr/bin/env python3

"""
MapReduce pipeline adapter for the NASA HTTP log analytics project.

This adapter:
- Uses shared configuration and staging
- Orchestrates the existing Java MapReduce jobs
- Produces the common JSONL output contract
- Does not connect directly to the database
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
import traceback
from collections import defaultdict
from pathlib import Path

# Ensure the project root is available when this adapter is run directly.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etl_common.config import (
    load_dotenv,
    load_config,
    default_input_files,
    default_batch_size,
    adapter_config,
    repo_path,
)

from etl_common.staging import write_staged_batches


def engine_info():
    """Print MapReduce engine information as JSON."""
    info = {
        "pipeline": "MapReduce",
        "engine": "Hadoop MapReduce",
        "execution_mode": "local",
        "adapter": "pipelines/mapreduce/adapter.py",
    }
    print(json.dumps(info, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(
        description="NASA HTTP log MapReduce adapter"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of physical records per batch",
    )

    parser.add_argument(
        "--output-dir",
        required=False,
        help="Directory for contract-compliant JSONL outputs",
    )

    parser.add_argument(
        "--input",
        action="append",
        default=None,
        help="Input log file. Repeat this argument to specify multiple files.",
    )

    parser.add_argument(
        "--debug-parsed",
        action="store_true",
        help="Write parsed_records.jsonl for debugging",
    )

    parser.add_argument(
        "--engine-info",
        action="store_true",
        help="Print engine information and exit",
    )

    return parser.parse_args()


def write_jsonl(path, records):
    """Write records as deterministic UTF-8 JSON Lines."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="\n") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            f.write("\n")


def read_part_files(output_dir):
    """Read non-empty lines from Hadoop part files in deterministic order."""
    output_dir = Path(output_dir)
    part_files = sorted(output_dir.glob("part-*"))

    if not part_files:
        raise RuntimeError(f"No Hadoop part files found in: {output_dir}")

    lines = []
    for part_file in part_files:
        with part_file.open("rb") as f:
            lines.extend(raw.decode("utf-8").removesuffix("\n") for raw in f if raw.rstrip(b"\n"))

    return lines


def run_hadoop_job(driver_class, input_path, output_dir, *driver_args):
    """Execute one existing Java MapReduce driver locally."""
    output_dir = Path(output_dir)
    jar_path = REPO_ROOT / "pipelines" / "mapreduce" / "build" / "mapreduce-parser.jar"

    if not jar_path.is_file():
        raise FileNotFoundError(f"Parser JAR not found: {jar_path}")

    if output_dir.exists():
        raise FileExistsError(
            f"Hadoop output directory already exists: {output_dir}"
        )

    command = [
        "hadoop",
        "jar",
        str(jar_path),
        driver_class,
        str(Path(input_path).resolve()),
        str(output_dir.resolve()),
        *[str(arg) for arg in driver_args],
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Hadoop job failed for {driver_class}.\\n"
            f"Command: {' '.join(command)}\\n"
            f"STDOUT:\\n{result.stdout}\\n"
            f"STDERR:\\n{result.stderr}"
        )

    return result


def run_q1(parsed_input, output_dir):
    """Run Q1 daily traffic aggregation and return aggregated records."""
    parsed_input = Path(parsed_input)

    if not parsed_input.exists():
        raise FileNotFoundError(f"Parsed input not found: {parsed_input}")

    run_hadoop_job(
        "nosql.mapreduce.q1.DailyTrafficDriver",
        parsed_input,
        output_dir,
    )

    records = []
    part_files = sorted(Path(output_dir).glob("part-*"))

    if not part_files:
        raise RuntimeError(f"Q1 produced no part files: {output_dir}")

    for part_file in part_files:
        with part_file.open("rb") as f:
            for raw in f:
                line = raw.decode("utf-8").removesuffix("\n").strip()
                if not line:
                    continue

                key, value = line.split("\t", 1)
                log_date, status_code = key.split("|", 1)
                request_count, total_bytes = value.split("|", 1)

                records.append({
                    "log_date": log_date,
                    "status_code": int(status_code),
                    "request_count": int(request_count),
                    "total_bytes": int(total_bytes),
                })

    return records



def run_q2(parsed_input, output_dir):
    """Run Q2 resource aggregation and return aggregated records."""
    parsed_input = Path(parsed_input)
    output_dir = Path(output_dir)

    if not parsed_input.exists():
        raise FileNotFoundError(f"Parsed input not found: {parsed_input}")

    run_hadoop_job(
        "nosql.mapreduce.q2.ResourceDriver",
        parsed_input,
        output_dir,
    )

    records = []
    part_files = sorted(output_dir.glob("part-*"))

    if not part_files:
        raise RuntimeError(f"Q2 produced no part files: {output_dir}")

    for part_file in part_files:
        with part_file.open("rb") as f:
            for raw in f:
                line = raw.decode("utf-8").removesuffix("\n").strip()
                if not line:
                    continue

                resource_path, value = line.split("\t", 1)
                request_count, total_bytes, distinct_hosts = value.split("|")

                records.append({
                    "resource_path": resource_path,
                    "request_count": int(request_count),
                    "total_bytes": int(total_bytes),
                    "distinct_host_count": int(distinct_hosts),
                })

    return records


def run_q3(parsed_input, output_dir):
    """Run Q3 hourly error aggregation and return aggregated records."""
    parsed_input = Path(parsed_input)
    output_dir = Path(output_dir)

    if not parsed_input.exists():
        raise FileNotFoundError(f"Parsed input not found: {parsed_input}")

    run_hadoop_job(
        "nosql.mapreduce.q3.HourlyErrorDriver",
        parsed_input,
        output_dir,
    )

    records = []
    part_files = sorted(output_dir.glob("part-*"))

    if not part_files:
        raise RuntimeError(f"Q3 produced no part files: {output_dir}")

    for part_file in part_files:
        with part_file.open("rb") as f:
            for raw in f:
                line = raw.decode("utf-8").removesuffix("\n").strip()
                if not line:
                    continue

                key, value = line.split("\t", 1)
                log_date, log_hour = key.split("|", 1)

                total_requests, errors, error_rate, distinct_error_hosts = (
                    value.split("|")
                )

                records.append({
                    "log_date": log_date,
                    "log_hour": int(log_hour),
                    "error_request_count": int(errors),
                    "total_request_count": int(total_requests),
                    "error_rate": float(error_rate),
                    "distinct_error_hosts": int(distinct_error_hosts),
                })

    return records

def run_top20(aggregation_input, output_dir):
    """Run global Top-20 ranking and return ranked resource records."""
    aggregation_input = Path(aggregation_input)
    output_dir = Path(output_dir)

    if not aggregation_input.exists():
        raise FileNotFoundError(
            f"Q2 aggregation input not found: {aggregation_input}"
        )

    run_hadoop_job(
        "nosql.mapreduce.q2.TopResourcesDriver",
        aggregation_input,
        output_dir,
    )

    records = []
    part_files = sorted(output_dir.glob("part-*"))

    if not part_files:
        raise RuntimeError(f"Top20 produced no part files: {output_dir}")

    for part_file in part_files:
        with part_file.open("rb") as f:
            for raw in f:
                line = raw.decode("utf-8").removesuffix("\n").strip()
                if not line:
                    continue

                rank, value = line.split("\t", 1)
                resource_path, metrics = value.split("\t", 1)

                request_count, total_bytes, distinct_hosts = metrics.split("|")

                records.append({
                    "rank": int(rank),
                    "resource_path": resource_path,
                    "request_count": int(request_count),
                    "total_bytes": int(total_bytes),
                    "distinct_host_count": int(distinct_hosts),
                })

    records.sort(key=lambda record: record["rank"])
    return records

def run_parser(staged_input, output_dir, batch_size):
    """Run Hadoop parsing on one staged batch and return parsed JSON records."""
    staged_input = Path(staged_input)
    output_dir = Path(output_dir)

    if not staged_input.is_file():
        raise FileNotFoundError(f"Staged input not found: {staged_input}")

    run_hadoop_job(
        "nosql.mapreduce.ParseLogsDriver",
        staged_input,
        output_dir,
        batch_size,
    )

    parsed_records = []
    part_files = sorted(output_dir.glob("part-*"))

    if not part_files:
        raise RuntimeError(
            f"Hadoop completed but produced no part files: {output_dir}"
        )

    for part_file in part_files:
        with part_file.open("rb") as f:
            for raw in f:
                line = raw.decode("utf-8").removesuffix("\n").strip()
                if line:
                    parsed_records.append(json.loads(line))

    return parsed_records


def process_batch(batch_id, staged_input, work_dir, batch_size):
    """Parse and aggregate one staged batch using Hadoop MapReduce."""
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    parser_dir = work_dir / "parser"
    q1_dir = work_dir / "q1"
    q2_dir = work_dir / "q2"
    q3_dir = work_dir / "q3"

    parsed_records = run_parser(
        staged_input,
        parser_dir,
        batch_size,
    )

    q1_records = run_q1(parser_dir, q1_dir)
    q2_records = run_q2(parser_dir, q2_dir)
    q3_records = run_q3(parser_dir, q3_dir)

    return {
        "batch_id": batch_id,
        "parsed_records": parsed_records,
        "q1_records": q1_records,
        "q2_records": q2_records,
        "q3_records": q3_records,
    }


def stage_inputs(input_files, batch_size, staging_dir):
    """Delegate record staging to the shared staging module."""
    return write_staged_batches(
        input_files=input_files,
        batch_size=batch_size,
        staging_dir=staging_dir,
    )


def resolve_runtime(args):
    """Load shared config and resolve runtime input/batch settings."""
    load_dotenv()
    config = load_config()

    inputs = (
        [repo_path(path) for path in args.input]
        if args.input
        else default_input_files(config)
    )

    batch_size = (
        args.batch_size
        if args.batch_size is not None
        else default_batch_size(config)
    )

    if batch_size <= 0:
        raise ValueError("Batch size must be a positive integer.")

    pipeline_config = adapter_config("MapReduce", config)

    return {
        "config": config,
        "inputs": inputs,
        "batch_size": batch_size,
        "pipeline_config": pipeline_config,
    }


def main():
    args = parse_args()

    if args.engine_info:
        engine_info()
        return 0

    try:
        runtime = resolve_runtime(args)
        if not args.output_dir:
            raise ValueError("--output-dir is required for execution")
        inputs = runtime["inputs"]
        missing = [str(p) for p in inputs if not p.is_file()]
        if missing:
            raise FileNotFoundError("Input file(s) not found: " + ", ".join(missing))
        out = Path(args.output_dir).resolve()
        out.mkdir(parents=True, exist_ok=False)
        work = out / "_work"
        staging = work / "staging"
        timings = []
        started = time.perf_counter()
        batch_meta = stage_inputs(inputs, runtime["batch_size"], staging)
        timings.append({"phase_name": "staging", "duration_seconds": time.perf_counter() - started})
        q1_rows, q2_rows, q3_rows, parsed_all = [], [], [], []
        batch_rows, samples = [], []
        process_start = time.perf_counter()
        for bid, count in batch_meta:
            stage_file = staging / f"batch_id={bid}" / "records.txt"
            # Hadoop's default record reader also splits on CR. The shared
            # contract treats only LF as a delimiter, so protect embedded CR
            # with a sentinel that cannot occur in ISO-8859-1 input; the Java
            # parser restores it before parsing.
            safe_stage = work / f"batch-{bid}" / "records.txt"
            safe_stage.parent.mkdir(parents=True, exist_ok=True)
            with stage_file.open("rb") as src, safe_stage.open("w", encoding="utf-8", newline="\n") as dst:
                for encoded in src:
                    dst.write(encoded.decode("utf-8").replace("\r", "\ue000"))
            result = process_batch(bid, safe_stage, work / f"batch-{bid}" / "jobs", runtime["batch_size"])
            parsed = sorted(result["parsed_records"], key=lambda r: r["record_number"])
            parsed_all.extend(parsed)
            malformed = [r for r in parsed if not r["valid"]]
            batch_rows.append({"batch_id": bid, "record_count": count, "malformed_count": len(malformed)})
            malformed_ids = {r["record_number"] for r in malformed} if len(samples) < 100 else set()
            raw_by_id = {}
            if malformed_ids:
                with stage_file.open("rb") as staged:
                    for encoded in staged:
                        line = encoded.decode("utf-8").removesuffix("\n")
                        n, _source, raw = line.split("\t", 2)
                        if int(n) in malformed_ids:
                            raw_by_id[int(n)] = raw.replace("\ue000", "\r")
            for r in malformed:
                if len(samples) == 100: break
                samples.append({"source_file": r["source_file"], "record_number": r["record_number"], "batch_id": bid,
                                "reason": r["reason"], "raw_line": raw_by_id.get(r["record_number"], "")})
            q1_rows.extend(dict(batch_id=bid, **r) for r in result["q1_records"])
            q2 = sorted(result["q2_records"], key=lambda r: (-r["request_count"], r["resource_path"].encode("utf-8")))[:20]
            q2_rows.extend(dict(batch_id=bid, rank=i, **r) for i, r in enumerate(q2, 1))
            q3_rows.extend(dict(batch_id=bid, **r) for r in result["q3_records"])

        # Whole-run Q1/Q3 aggregate from batch outputs. For Q2, re-aggregate all
        # parsed records in Hadoop so distinct hosts are deduplicated globally.
        q1_global = defaultdict(lambda: [0, 0])
        for r in q1_rows:
            a=q1_global[(r["log_date"],r["status_code"])]; a[0]+=r["request_count"]; a[1]+=r["total_bytes"]
        q1_rows += [dict(batch_id=None,log_date=k[0],status_code=k[1],request_count=v[0],total_bytes=v[1]) for k,v in sorted(q1_global.items())]
        q3_global = defaultdict(lambda: [0,0,set()])
        for r in parsed_all:
            if r["valid"]:
                a=q3_global[(r["log_date"],r["log_hour"])]
                a[1]+=1
                if 400 <= r["status_code"] <= 599:
                    a[0]+=1; a[2].add(r["host"])
        q3_rows += [dict(batch_id=None,log_date=k[0],log_hour=k[1],error_request_count=v[0],total_request_count=v[1],error_rate=v[0]/v[1],distinct_error_hosts=len(v[2])) for k,v in sorted(q3_global.items())]
        global_top=[]
        if parsed_all:
            merged=work / "all-parsed.jsonl"
            write_jsonl(merged, parsed_all)
            agg=work / "global-q2"; run_q2(merged, agg)
            global_top=run_top20(agg, work / "global-top20")
        q2_rows += [dict(batch_id=None, **r) for r in global_top]
        timings.append({"phase_name":"processing","duration_seconds":time.perf_counter()-process_start})
        outputs={"batches.jsonl":batch_rows,"daily_traffic.jsonl":q1_rows,"top_resources.jsonl":q2_rows,
                 "hourly_errors.jsonl":q3_rows,"malformed_samples.jsonl":samples}
        if args.debug_parsed: outputs["parsed_records.jsonl"] = parsed_all
        for name, rows in outputs.items(): write_jsonl(out / name, rows)
        write_jsonl(out / "phase_timings.jsonl", timings)
        print(f"MapReduce completed: {len(batch_rows)} batches, {sum(x['record_count'] for x in batch_rows)} records; outputs: {out}")

    except Exception as exc:
        print(f"MapReduce adapter initialization failed: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
