"""Command-line orchestration for one selected analytics pipeline."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from controller.adapters import StartError, canonical_name, launcher
from controller.validation import validate_output
from etl_common.config import REPO_ROOT, default_batch_size, default_input_files, load_config, load_dotenv

BATCH_CHOICES = (100_000, 500_000, 1_000_000)


def _interactive(config):
    configured = config["pipelines"]["adapters"]
    preferred = ("MapReduce", "Pig", "Hive", "MongoDB")
    by_name = {item["name"].lower(): item for item in configured}
    adapters = [by_name[name.lower()] for name in preferred if name.lower() in by_name]
    print("=" * 40 + "\n       NASA LOG ANALYTICS PROJECT\n" + "=" * 40)
    print("\nSelect Pipeline:")
    for i, item in enumerate(adapters, 1): print(f"{i}. {item['name']}")
    choice = int(input("\nYour choice: "))
    if choice < 1 or choice > len(adapters): raise ValueError("Invalid pipeline choice")
    pipeline = adapters[choice - 1]["name"]
    print("\nSelect Batch Size:\n1. 100,000\n2. 500,000\n3. 1,000,000\n4. Custom")
    size_choice = int(input("\nYour choice: "))
    batch = BATCH_CHOICES[size_choice - 1] if size_choice in (1, 2, 3) else int(input("Custom batch size: "))
    return pipeline, batch


def _db_connect():
    try:
        import psycopg
    except ImportError as e:
        raise RuntimeError("PostgreSQL loading requires psycopg; install requirements.txt") from e
    try:
        return psycopg.connect(
            host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5432"),
            user=os.getenv("PGUSER", "etl"), password=os.getenv("PGPASSWORD", ""),
            dbname=os.getenv("PGDATABASE", "nosql_final_project"),
        )
    except Exception as e:
        raise RuntimeError(f"Could not connect to PostgreSQL: {e}. Check .env and start the database.") from e


def _rows(out, name):
    p = out / name
    if not p.exists(): return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines()]


def _load_results(run_id, pipeline, started_at, out, totals, inputs):
    """Persist all contract outputs in one result transaction."""
    connection = _db_connect()
    try:
        with connection.transaction():
            with connection.cursor() as cur:
                cur.executemany("""INSERT INTO run_batches(run_id,batch_id,record_count,malformed_count) VALUES (%s,%s,%s,%s)""",
                                [(run_id,r["batch_id"],r["record_count"],r["malformed_count"]) for r in _rows(out,"batches.jsonl")])
                for fname in ("phase_timings.jsonl",):
                    cur.executemany("INSERT INTO phase_timings(run_id,phase_name,duration_seconds) VALUES (%s,%s,%s)",
                                    [(run_id,r["phase_name"],r["duration_seconds"]) for r in _rows(out,fname)])
                cur.executemany("INSERT INTO malformed_samples(run_id,record_number,batch_id,source_file,reason,raw_line) VALUES (%s,%s,%s,%s,%s,%s)",
                                [(run_id,r["record_number"],r["batch_id"],r["source_file"],r["reason"],r["raw_line"].replace("\x00",r"\u0000")) for r in _rows(out,"malformed_samples.jsonl")])
                specs = {
                    "daily_traffic.jsonl": ("q1_daily_traffic", ["log_date","status_code","request_count","total_bytes"]),
                    "top_resources.jsonl": ("q2_top_resources", ["rank","resource_path","request_count","total_bytes","distinct_host_count"]),
                    "hourly_errors.jsonl": ("q3_hourly_errors", ["log_date","log_hour","error_request_count","total_request_count","error_rate","distinct_error_hosts"]),
                }
                for fname,(table,fields) in specs.items():
                    columns=["run_id","pipeline_name","batch_id","executed_at"]+fields
                    sql=f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})"
                    values=[]
                    for r in _rows(out,fname):
                        vals=[run_id,pipeline,r["batch_id"],started_at]+[r[f] for f in fields]
                        if fname == "hourly_errors.jsonl": vals[-2]=str(vals[-2])
                        values.append(tuple(vals))
                    cur.executemany(sql,values)
                cur.execute("UPDATE runs SET total_records=%s,total_batches=%s,malformed_count=%s,average_batch_size=%s,results_committed_at=now() WHERE run_id=%s",
                            (totals["total_records"],totals["total_batches"],totals["malformed_count"],totals["average_batch_size"],run_id))
        return connection
    except Exception:
        connection.close()
        raise


def execute(pipeline, batch_size, inputs=None, output_dir=None, no_db=False, debug_parsed=False):
    pipeline = canonical_name(pipeline)
    inputs = [Path(p).resolve() for p in (inputs or default_input_files())]
    if batch_size <= 0: raise ValueError("Batch size must be a positive integer")
    for path in inputs:
        if not path.is_file(): raise FileNotFoundError(f"Input file not found: {path}")
    run_id = str(uuid.uuid4())
    out = Path(output_dir).resolve() if output_dir else REPO_ROOT / "results" / run_id
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists(): raise FileExistsError(f"Run output already exists: {out}")
    # Resolve the launcher before creating a RUNNING database row. This keeps
    # the menu usable while a pipeline is still under development.
    cmd = launcher(pipeline) + ["--batch-size",str(batch_size),"--output-dir",str(out)]
    for p in inputs: cmd += ["--input",str(p)]
    if debug_parsed: cmd.append("--debug-parsed")
    started = datetime.now(timezone.utc)
    db = None
    if not no_db:
        db = _db_connect()
        files = [{"path":str(p),"size_bytes":p.stat().st_size,"sha256":hashlib.sha256(p.read_bytes()).hexdigest()} for p in inputs]
        with db.transaction():
            with db.cursor() as cur:
                cur.execute("INSERT INTO runs(run_id,pipeline_name,batch_size,status,started_at,input_files) VALUES (%s,%s,%s,'RUNNING',%s,%s::jsonb)",
                            (run_id,pipeline,batch_size,started,json.dumps(files)))
        db.close(); db=None
    print(f"Starting {pipeline}...\nRun ID: {run_id}\nOutput: {out}")
    t0=time.perf_counter()
    try:
        proc=subprocess.run(cmd,cwd=REPO_ROOT,text=True)
        if proc.returncode: raise RuntimeError(f"Adapter exited with status {proc.returncode}")
        check=validate_output(out,batch_size,require_parsed=debug_parsed)
        if not check.ok: raise RuntimeError("Output validation failed:\n"+"\n".join(check.errors[:30]))
        if not no_db:
            db=_load_results(run_id,pipeline,started,out,check.totals,inputs)
            elapsed=time.perf_counter()-t0
            db.close(); db=None
            db=_db_connect()
            with db.transaction():
                with db.cursor() as cur:
                    cur.execute("UPDATE runs SET elapsed_seconds=%s,completed_at=now(),status='COMPLETED' WHERE run_id=%s",(elapsed,run_id))
            db.close(); db=None
        else:
            elapsed=time.perf_counter()-t0
        avg = check.totals["average_batch_size"]
        avg_text = f"{avg:.1f}" if avg is not None else "0"
        print(f"SUCCESS: {pipeline}; {check.totals['total_records']} records; "
              f"{check.totals['total_batches']} batches; "
              f"{check.totals['malformed_count']} malformed; "
              f"average batch {avg_text}; {elapsed:.3f}s" +
              (" (database not loaded)" if no_db else "; results stored in PostgreSQL"))
        return 0
    except Exception as exc:
        if db: db.close()
        if not no_db:
            try:
                db=_db_connect()
                with db.transaction():
                    with db.cursor() as cur: cur.execute("UPDATE runs SET status='FAILED',error_message=%s WHERE run_id=%s",(str(exc)[:8000],run_id))
            except Exception as db_exc: print(f"Could not record failure in PostgreSQL: {db_exc}",file=sys.stderr)
            finally:
                if db: db.close()
        print(f"FAILED: {exc}",file=sys.stderr)
        return 1


def main(argv=None):
    load_dotenv()
    config=load_config()
    ap=argparse.ArgumentParser(description="Run one NASA log analytics pipeline")
    ap.add_argument("--pipeline",choices=[a["name"].lower() for a in config["pipelines"]["adapters"]])
    ap.add_argument("--batch-size",type=int)
    ap.add_argument("--input",action="append",help="Input path; repeat to preserve source order")
    ap.add_argument("--output-dir",help="Fresh output directory (default: results/<run-id>)")
    ap.add_argument("--no-db",action="store_true",help="Execute and validate without PostgreSQL persistence")
    ap.add_argument("--debug-parsed",action="store_true",help="Ask the adapter to write parsed_records.jsonl")
    args=ap.parse_args(argv)
    try:
        pipeline=args.pipeline
        batch=args.batch_size
        if not pipeline: pipeline,batch=_interactive(config)
        elif batch is None: batch=default_batch_size(config)
        return execute(pipeline,batch,args.input,args.output_dir,args.no_db,args.debug_parsed)
    except (ValueError,StartError,FileNotFoundError,FileExistsError,RuntimeError) as exc:
        print(f"Cannot start run: {exc}",file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
