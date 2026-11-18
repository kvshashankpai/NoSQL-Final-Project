"""Run Pig through the shared PostgreSQL schema, or read/recover a run."""

import argparse
import json
import math
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import UUID, uuid4

from controller.adapters import launcher
from controller.validation import validate_output
from etl_common.config import (REPO_ROOT, default_batch_size, default_input_files, load_config,
                               load_dotenv, repo_path)
from scripts.verify_inputs import VERIFICATION_DIR, check_current, load_latest

PIPELINE = "Pig"
RUNS = REPO_ROOT / "runs"
QUERY_TABLES = {
    "daily_traffic.jsonl": ("q1_daily_traffic", ("log_date", "status_code", "request_count",
                                                "total_bytes"), "log_date, status_code"),
    "top_resources.jsonl": ("q2_top_resources", ("rank", "resource_path", "request_count",
                                                "total_bytes", "distinct_host_count"), "rank"),
    "hourly_errors.jsonl": ("q3_hourly_errors", ("log_date", "log_hour", "error_request_count",
                                                "total_request_count", "error_rate",
                                                "distinct_error_hosts"), "log_date, log_hour"),
}


def acquire_run_lock(conn, run_id):
    locked, = conn.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (str(run_id),)
    ).fetchone()
    if not locked:
        raise ValueError(f"Run {run_id} is still owned by an active controller.")


def insert_rows(conn, table, columns, values):
    # All identifiers come from literals or QUERY_TABLES above.
    names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    with conn.cursor() as cursor:
        cursor.executemany(f"INSERT INTO public.{table} ({names}) VALUES ({placeholders})", values)


def store_results(conn, run_id, started_at, result):
    data = result.data
    with conn.transaction():
        insert_rows(conn, "run_batches", ("run_id", "batch_id", "record_count", "malformed_count"),
                    ((run_id, row["batch_id"], row["record_count"], row["malformed_count"])
                     for row in data["batches.jsonl"]))
        insert_rows(conn, "malformed_samples",
                    ("run_id", "record_number", "batch_id", "source_file", "reason", "raw_line"),
                    ((run_id, row["record_number"], row["batch_id"], row["source_file"],
                      row["reason"], row["raw_line"].replace("\0", r"\u0000"))
                     for row in data["malformed_samples.jsonl"]))
        insert_rows(conn, "phase_timings", ("run_id", "phase_name", "duration_seconds"),
                    ((run_id, row["phase_name"], row["duration_seconds"])
                     for row in data.get("phase_timings.jsonl", ())))
        for name, (table, fields, _) in QUERY_TABLES.items():
            insert_rows(conn, table, ("run_id", "pipeline_name", "batch_id", "executed_at") + fields,
                        ((run_id, PIPELINE, row["batch_id"], started_at, *(row[f] for f in fields))
                         for row in data[name]))
        conn.execute(
            """UPDATE public.runs SET total_records = %(total_records)s,
               total_batches = %(total_batches)s, malformed_count = %(malformed_count)s,
               average_batch_size = %(average_batch_size)s, results_committed_at = clock_timestamp()
               WHERE run_id = %(run_id)s""",
            {"run_id": run_id, **result.totals},
        )


def write_completion(directory, run_id, elapsed, completed):
    temporary = directory / "completion.tmp"
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump({"run_id": str(run_id), "elapsed_seconds": elapsed,
                   "completed_at": completed.isoformat()}, stream, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(directory / "completion.json")
    if os.name == "posix":
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def publish(conn, run_id, elapsed, completed):
    changed = conn.execute(
        """UPDATE public.runs SET elapsed_seconds = %s, completed_at = %s, status = 'COMPLETED'
           WHERE run_id = %s AND status = 'RUNNING' AND results_committed_at IS NOT NULL""",
        (elapsed, completed, run_id),
    )
    if changed.rowcount != 1:
        raise ValueError(f"Run {run_id} is not ready for publication.")


def record_failure(conn, run_id, message, *, allow_committed=False):
    conn.execute(
        """UPDATE public.runs SET status = 'FAILED', error_message = %s
           WHERE run_id = %s AND status = 'RUNNING'
           AND (%s OR results_committed_at IS NULL)""",
        (message.replace("\0", r"\u0000"), run_id, allow_committed),
    )


def recover(conn, run_id):
    acquire_run_lock(conn, run_id)
    row = conn.execute(
        "SELECT status, started_at, results_committed_at FROM public.runs "
        "WHERE run_id = %s AND pipeline_name = %s", (run_id, PIPELINE),
    ).fetchone()
    if row is None:
        raise ValueError(f"Pig run {run_id} does not exist.")
    status, started, committed = row
    if status != "RUNNING":
        return status
    if committed is not None:
        try:
            record = json.loads((RUNS / str(run_id) / "completion.json").read_text(encoding="utf-8"))
            elapsed = record["elapsed_seconds"]
            completed = datetime.fromisoformat(record["completed_at"])
            if (record["run_id"] != str(run_id) or type(elapsed) not in (float, int)
                    or not math.isfinite(elapsed) or elapsed < 0 or completed.tzinfo is None
                    or completed < started):
                raise ValueError("Invalid completion metadata")
        except (OSError, KeyError, TypeError, ValueError, OverflowError):
            pass
        else:
            publish(conn, run_id, elapsed, completed)
            return "COMPLETED"
    record_failure(conn, run_id, "Controller stopped without recoverable completion metadata.",
                   allow_committed=True)
    return "FAILED"


def report(conn, run_id):
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """SELECT run_id, pipeline_name, batch_size, started_at, completed_at,
               elapsed_seconds, total_records, total_batches, malformed_count, average_batch_size
               FROM public.runs WHERE run_id = %s AND pipeline_name = %s AND status = 'COMPLETED'""",
            (run_id, PIPELINE),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Completed Pig run {run_id} does not exist.")
        cursor.execute(
            "SELECT batch_id, record_count, malformed_count FROM public.run_batches "
            "WHERE run_id = %s ORDER BY batch_id", (run_id,),
        )
        row["batches"] = cursor.fetchall()
        for table, fields, order in QUERY_TABLES.values():
            cursor.execute(f"SELECT {', '.join(fields)} FROM public.{table} "
                           f"WHERE run_id = %s AND batch_id IS NULL ORDER BY {order}", (run_id,))
            row[table] = cursor.fetchall()
        return row


def execute(conn, inputs, batch_size, verification):
    from psycopg.types.json import Jsonb

    command = launcher(PIPELINE)
    env_info = {"platform": platform.platform(), "architecture": platform.machine(),
                "python": platform.python_version(), "postgresql": conn.info.server_version}
    capabilities = subprocess.run(command + ["--engine-info"], cwd=REPO_ROOT, check=True,
                                  stdout=subprocess.PIPE, encoding="utf-8")
    engine = json.loads(capabilities.stdout)
    if not isinstance(engine, dict):
        raise ValueError("Pig engine information must be a JSON object.")
    run_id = uuid4()
    directory = RUNS / str(run_id)
    directory.mkdir(parents=True)
    output = directory / "output"
    command += ["--batch-size", str(batch_size), "--output-dir", str(output)]
    for path in inputs:
        command.extend(("--input", str(path)))
    acquire_run_lock(conn, run_id)
    print(f"Starting Pig run {run_id}", flush=True)
    started_at = datetime.now(timezone.utc)
    start = perf_counter()
    try:
        conn.execute(
            """INSERT INTO public.runs
               (run_id, pipeline_name, batch_size, status, started_at, input_files, engine_info, environment)
               VALUES (%s, %s, %s, 'RUNNING', %s, %s, %s, %s)""",
            (run_id, PIPELINE, batch_size, started_at, Jsonb(verification["files"]),
             Jsonb(engine), Jsonb(env_info)),
        )
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        problems = check_current(verification, inputs)
        if problems:
            raise ValueError("\n".join(problems))
        result = validate_output(output, batch_size)
        if not result.ok:
            raise ValueError("Invalid adapter output:\n" + "\n".join(result.errors))
        store_results(conn, run_id, started_at, result)
        elapsed = perf_counter() - start
        completed_at = datetime.now(timezone.utc)
        write_completion(directory, run_id, elapsed, completed_at)
        publish(conn, run_id, elapsed, completed_at)
    except (Exception, KeyboardInterrupt) as error:
        try:
            if not conn.closed:
                record_failure(conn, run_id, str(error) or type(error).__name__)
        except Exception as failure_error:
            print(f"Run status update failed: {failure_error}", file=sys.stderr)
        print(f"Run {run_id} stopped. Use --recover {run_id} to resolve a pending run.",
              file=sys.stderr)
        raise
    return run_id


def main(argv=None):
    load_dotenv()
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--report", type=UUID, help="Read a completed run from PostgreSQL.")
    action.add_argument("--recover", type=UUID, help="Recover a specific abandoned run.")
    parser.add_argument("--batch-size", type=int, default=default_batch_size(config))
    parser.add_argument("--input", action="append", type=Path, help="Repeat in input order.")
    parser.add_argument("--verification-dir", type=Path, default=VERIFICATION_DIR)
    args = parser.parse_args(argv)
    try:
        import psycopg
    except ImportError:
        parser.error("Install PostgreSQL support with python -m pip install -r requirements.txt")

    try:
        if args.report is None and args.recover is None:
            if args.batch_size <= 0:
                parser.error("--batch-size must be positive")
            inputs = [repo_path(path) for path in args.input] if args.input else default_input_files(config)
            verification = load_latest(args.verification_dir)
            if verification is None:
                raise ValueError("Input verification is missing. Run python -m scripts.verify_inputs first.")
            problems = check_current(verification, inputs)
            if problems:
                raise ValueError("\n".join(problems))
        database = os.environ.get("PGDATABASE", config["database"]["database_name"])
        with psycopg.connect(dbname=database, autocommit=True) as conn:
            if args.recover is not None:
                print(f"{args.recover}: {recover(conn, args.recover)}")
            else:
                run_id = args.report or execute(conn, inputs, args.batch_size, verification)
                print(json.dumps(report(conn, run_id), indent=2, default=str))
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError, psycopg.Error) as error:
        print(f"Pig run: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
