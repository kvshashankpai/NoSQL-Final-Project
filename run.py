"""Run one pipeline end to end and load its results into PostgreSQL.

    python run.py --pipeline MapReduce --input JUL --input AUG [--batch-size N]
    python run.py --list                      # every stored run
    python run.py --report RUN_ID [--full]    # one stored run
    python run.py --recover RUN_ID            # resolve a run left RUNNING

The runtime is measured from the adapter start until the result transaction
commits (input verification and the engine version check are not timed).
The Hadoop execution mode comes from the config or HADOOP_EXECUTION_MODE.
Generalises pipelines/pig/run.py (same schema and transaction rules) to both pipelines.
"""

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

from controller.adapters import canonical_name, launcher
from controller.validation import validate_output
from etl_common.config import (REPO_ROOT, default_batch_size, default_input_files, load_config,
                               load_dotenv, repo_path)
from scripts.verify_inputs import build_record, check_current, load_latest, write_record

PIPELINES = ("Pig", "MapReduce")
# Job output stays on the Linux file system: /mnt/c and OneDrive I/O would distort timings.
RUNS = Path(os.environ.get("NOSQL_RUNS_DIR", Path.home() / "nosql-runs"))
VERIFICATION = Path(os.environ.get("NOSQL_VERIFICATION_DIR", Path.home() / "nosql-verification"))
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


def store_results(conn, run_id, pipeline, started_at, result):
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
                        ((run_id, pipeline, row["batch_id"], started_at, *(row[f] for f in fields))
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
        "SELECT status, started_at, results_committed_at FROM public.runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Run {run_id} does not exist.")
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
            """SELECT run_id, pipeline_name, engine_info->>'execution_mode' AS execution_mode,
               batch_size, started_at, completed_at, elapsed_seconds, total_records,
               total_batches, malformed_count, average_batch_size
               FROM public.runs WHERE run_id = %s AND status = 'COMPLETED'""",
            (run_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Completed run {run_id} does not exist.")
        cursor.execute(
            "SELECT batch_id, record_count, malformed_count FROM public.run_batches "
            "WHERE run_id = %s ORDER BY batch_id", (run_id,),
        )
        row["batches"] = cursor.fetchall()
        cursor.execute(
            "SELECT phase_name, duration_seconds FROM public.phase_timings "
            "WHERE run_id = %s ORDER BY timing_id", (run_id,),
        )
        row["phase_timings"] = cursor.fetchall()
        for table, fields, order in QUERY_TABLES.values():
            cursor.execute(f"SELECT {', '.join(fields)} FROM public.{table} "
                           f"WHERE run_id = %s AND batch_id IS NULL ORDER BY {order}", (run_id,))
            row[table] = cursor.fetchall()
        return row


def summary(row):
    """A short, readable run report. --full prints every result row instead."""
    lines = [
        f"Run {row['run_id']}",
        f"  pipeline        {row['pipeline_name']} ({row['execution_mode']} mode)",
        f"  batch size      {row['batch_size']:,}",
        f"  elapsed         {row['elapsed_seconds']:.1f} s  (adapter start to result commit)",
        f"  records         {row['total_records']:,}  in {row['total_batches']} batches, "
        f"average batch size {row['average_batch_size'] or 0:,.2f}",
        f"  malformed       {row['malformed_count']:,}",
    ]
    for phase in row["phase_timings"]:
        lines.append(f"  phase {phase['phase_name']:<10}{phase['duration_seconds']:.1f} s")
    lines.append("  batches         " + ", ".join(
        f"{b['batch_id']}: {b['record_count']:,} ({b['malformed_count']} malformed)"
        for b in row["batches"]))
    q1, q2, q3 = row["q1_daily_traffic"], row["q2_top_resources"], row["q3_hourly_errors"]
    days = sorted({r["log_date"] for r in q1})
    lines.append(f"  Q1 daily traffic   {len(q1)} rows, {len(days)} days"
                 + (f" ({days[0]} to {days[-1]})" if days else ""))
    lines.append(f"  Q2 top resources   {len(q2)} rows; top 5:")
    for r in q2[:5]:
        lines.append(f"      {r['rank']:>2}. {r['resource_path']}  {r['request_count']:,} requests, "
                     f"{r['distinct_host_count']:,} hosts")
    lines.append(f"  Q3 hourly errors   {len(q3)} hours (an error is HTTP status 400-599)")
    if q3:
        worst = max(q3, key=lambda r: (r["error_rate"], r["error_request_count"]))
        lines.append(f"      highest error rate {float(worst['error_rate']):.4f} on {worst['log_date']} "
                     f"{worst['log_hour']:02d}:00 ({worst['error_request_count']} of "
                     f"{worst['total_request_count']} requests)")
    return "\n".join(lines)


def list_runs(conn):
    rows = conn.execute(
        """SELECT run_id, pipeline_name, engine_info->>'execution_mode', status, batch_size,
           elapsed_seconds, total_records, started_at FROM public.runs ORDER BY started_at"""
    ).fetchall()
    print(f"{'run_id':<36}  {'pipeline':<9}  {'mode':<5}  {'status':<9}  {'batch':>9}  "
          f"{'elapsed_s':>9}  {'records':>9}  started (UTC)")
    for run_id, name, mode, status, batch, elapsed, records, started in rows:
        print(f"{str(run_id):<36}  {name:<9}  {mode or '-':<5}  {status:<9}  {batch:>9,}  "
              f"{'' if elapsed is None else f'{elapsed:.1f}':>9}  "
              f"{'' if records is None else records:>9}  {started:%Y-%m-%d %H:%M:%S}")


def verified_inputs(inputs, encoding):
    """Reuse the latest verification if it still matches; otherwise hash again (not timed)."""
    verification = load_latest(VERIFICATION)
    if verification is None or check_current(verification, inputs):
        print("Verifying input files (SHA-256, not part of the timed run)...", flush=True)
        verification = build_record(inputs, encoding)
        write_record(verification, VERIFICATION)
    return verification


def execute(conn, pipeline, inputs, batch_size, verification):
    from psycopg.types.json import Jsonb

    command = launcher(pipeline)
    env_info = {"platform": platform.platform(), "architecture": platform.machine(),
                "python": platform.python_version(), "postgresql": conn.info.server_version}
    capabilities = subprocess.run(command + ["--engine-info"], cwd=REPO_ROOT, check=True,
                                  stdout=subprocess.PIPE, encoding="utf-8")
    engine = json.loads(capabilities.stdout)
    if not isinstance(engine, dict):
        raise ValueError(f"{pipeline} engine information must be a JSON object.")
    run_id = uuid4()
    directory = RUNS / str(run_id)
    directory.mkdir(parents=True)
    output = directory / "output"
    command += ["--batch-size", str(batch_size), "--output-dir", str(output)]
    for path in inputs:
        command.extend(("--input", str(path)))
    acquire_run_lock(conn, run_id)
    print(f"Starting {pipeline} run {run_id} ({engine.get('execution_mode')} mode, "
          f"batch size {batch_size:,}); engine log: {directory}/adapter.log", flush=True)
    started_at = datetime.now(timezone.utc)
    start = perf_counter()
    try:
        conn.execute(
            """INSERT INTO public.runs
               (run_id, pipeline_name, batch_size, status, started_at, input_files, engine_info, environment)
               VALUES (%s, %s, %s, 'RUNNING', %s, %s, %s, %s)""",
            (run_id, pipeline, batch_size, started_at, Jsonb(verification["files"]),
             Jsonb(engine), Jsonb(env_info)),
        )
        with (directory / "adapter.log").open("wb") as log:
            adapter = subprocess.run(command, cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT)
        if adapter.returncode != 0:
            tail = (directory / "adapter.log").read_bytes()[-2000:].decode("utf-8", "replace")
            raise RuntimeError(f"{pipeline} adapter exited with {adapter.returncode}:\n{tail}")
        problems = check_current(verification, inputs)
        if problems:
            raise ValueError("\n".join(problems))
        result = validate_output(output, batch_size)
        if not result.ok:
            raise ValueError("Invalid adapter output:\n" + "\n".join(result.errors[:30]))
        store_results(conn, run_id, pipeline, started_at, result)
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
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--pipeline", help="Pig or MapReduce")
    action.add_argument("--report", type=UUID, help="Show a completed run.")
    action.add_argument("--recover", type=UUID, help="Recover a specific abandoned run.")
    action.add_argument("--list", action="store_true", help="List every stored run.")
    parser.add_argument("--batch-size", type=int, default=default_batch_size(config))
    parser.add_argument("--input", action="append", type=Path, help="Repeat in input order.")
    parser.add_argument("--full", action="store_true", help="Print every result row as JSON.")
    args = parser.parse_args(argv)
    try:
        import psycopg
    except ImportError:
        parser.error("psycopg is missing: python3 -m pip install --target ~/etlpy 'psycopg[binary]' "
                     "and set PYTHONPATH=~/etlpy (env.sh does this)")

    try:
        pipeline = None
        if args.pipeline:
            pipeline = canonical_name(args.pipeline)
            if pipeline not in PIPELINES:
                parser.error(f"--pipeline must be one of {', '.join(PIPELINES)}")
            if args.batch_size <= 0:
                parser.error("--batch-size must be positive")
            inputs = [repo_path(p) for p in args.input] if args.input else default_input_files(config)
            missing = [str(p) for p in inputs if not p.is_file()]
            if missing:
                raise ValueError("Input file(s) not found: " + ", ".join(missing))
            verification = verified_inputs(inputs, config["dataset"]["encoding"])
        database = os.environ.get("PGDATABASE", config["database"]["database_name"])
        with psycopg.connect(dbname=database, autocommit=True) as conn:
            if args.list:
                list_runs(conn)
            elif args.recover is not None:
                print(f"{args.recover}: {recover(conn, args.recover)}")
            else:
                run_id = args.report or execute(conn, pipeline, inputs, args.batch_size, verification)
                row = report(conn, run_id)
                print(json.dumps(row, indent=2, default=str) if args.full else summary(row))
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError, psycopg.Error) as error:
        print(f"run: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
