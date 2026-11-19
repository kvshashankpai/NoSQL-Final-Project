"""Run one complete Pig adapter invocation in the shared Hadoop execution mode.

yarn: HDFS and YARN (pig -x mapreduce). local: local file system (pig -x local).
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from contextlib import ExitStack
from itertools import chain, groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from uuid import uuid4

from controller.outputs import REQUIRED_FILES, SAMPLE_LIMIT, TOP_N
from etl_common.config import (default_batch_size, default_input_files, hadoop_execution_mode,
                               load_dotenv, repo_path)
from etl_common.staging import iter_records, staged_line

HERE = Path(__file__).resolve().parent
QUERIES = {
    "q1": "daily_traffic.jsonl",
    "q2": "top_resources.jsonl",
    "q3": "hourly_errors.jsonl",
}


def executable(home, name):
    root = os.environ.get(home)
    path = str(Path(root).expanduser().resolve() / "bin" / name) if root else name
    full_path = shutil.which(path)
    if full_path is None:
        raise ValueError(f"{name} was not found. Set {home} (see env.sh).")
    return full_path


def engine_info():
    commands = {
        "pig": [executable("PIG_HOME", "pig"), "-version"],
        "hadoop": [executable("HADOOP_HOME", "hadoop"), "version"],
        "java": [executable("JAVA_HOME", "java"), "-version"],
    }
    versions = {"execution_mode": hadoop_execution_mode()}
    for name, command in commands.items():
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                check=True, encoding="utf-8", errors="replace")
        line = next((line for line in result.stdout.splitlines()
                     if line.lower().startswith(("apache pig version", "hadoop ", "openjdk version",
                                                 "java version"))), None)
        if line is None:
            raise ValueError(f"Could not identify the {name} version from {command[0]}")
        versions[name] = line
    return versions


class PigJob:
    def __init__(self, work, debug):
        self.work = work
        self.hdfs = executable("HADOOP_HOME", "hdfs")
        self.pig = executable("PIG_HOME", "pig")
        self.root = f"/tmp/nosql-pig-{uuid4().hex}"
        self.log = work / "engine.log"
        local = hadoop_execution_mode() == "local"
        self.exec_type = "local" if local else "mapreduce"
        # In local mode the job paths are on the local file system, so point
        # the hdfs dfs commands there as well.
        self.fs = ["-fs", "file:///"] if local else []

        jar = HERE / "build" / "pig-helpers.jar"
        if not jar.is_file():
            raise ValueError("Missing Pig helper JAR. Run bash pipelines/pig/build.sh first.")
        shutil.copyfile(jar, work / jar.name)
        queries = (HERE / "queries.pig").read_text(encoding="utf-8")
        batch = (HERE / "batch.pig").read_text(encoding="utf-8")
        if debug:
            batch += "\n" + (HERE / "debug.pig").read_text(encoding="utf-8")
        whole = (HERE / "whole_run.pig").read_text(encoding="utf-8")
        (work / "batch-job.pig").write_text(batch + "\n" + queries, encoding="utf-8", newline="\n")
        (work / "whole-job.pig").write_text(whole + "\n" + queries, encoding="utf-8", newline="\n")

    def command(self, *args):
        with self.log.open("ab") as log:
            log.write(("\n$ " + repr(args) + "\n").encode("utf-8"))
            log.flush()
            result = subprocess.run(args, cwd=self.work, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode != 0:
            with self.log.open("rb") as log:
                size = log.seek(0, os.SEEK_END)
                log.seek(max(0, size - 1600))
                tail = log.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{Path(args[0]).name} exited with {result.returncode}.\n{tail}")

    def dfs(self, *args):
        self.command(self.hdfs, "dfs", *self.fs, *args)

    def run(self, script, result, **parameters):
        options = {"ROOT": self.root, "RESULT": result, "TOP_N": TOP_N, **parameters}
        args = ["-x", self.exec_type, "-logfile", "pig.log"]
        for key, value in options.items():
            args.extend(("-param", f"{key}={value}"))
        self.command(self.pig, *args, "-f", script)


def rows(directory):
    if not directory.is_dir():
        raise ValueError(f"Pig output directory is missing: {directory.name}")
    for part in sorted(directory.glob("part-*")):
        with part.open(encoding="utf-8", newline="\n") as stream:
            for line in stream:
                yield json.loads(line)


def emit(stream, row):
    stream.write(json.dumps(row, ensure_ascii=True, allow_nan=False) + "\n")


def write_queries(streams, result, batch_id):
    for key, output in QUERIES.items():
        records = rows(result / key)
        if key == "q2":
            # Hadoop part order is not a public output contract. Pig already
            # selected the top N. Number those rows after sorting the candidates.
            top = sorted(records, key=lambda row: (-row["request_count"],
                                                   row["resource_path"].encode("utf-8")))
            for rank, row in enumerate(top, 1):
                emit(streams[output], {"batch_id": batch_id, "rank": rank, **row})
        else:
            for row in records:
                emit(streams[output], {"batch_id": batch_id, **row})


def process_batches(job, records, streams, debug, timings):
    """timings collects seconds per phase: staging (write and upload the batch),
    pig (Pig jobs) and collect (download and package the results)."""
    staged = job.work / "records.txt"
    clock = perf_counter()

    def lap(phase):
        nonlocal clock
        now = perf_counter()
        timings[phase] = timings.get(phase, 0.0) + now - clock
        clock = now
    samples_written = 0
    valid_count = 0
    for batch_id, batch in groupby(records, key=lambda record: record.batch_id):
        count = 0
        with staged.open("w", encoding="utf-8", newline="\n") as stream:
            for record in batch:
                stream.write(staged_line(record))
                count += 1
        source = f"{job.root}/records.txt"
        result = f"{job.root}/result-{batch_id}"
        job.dfs("-put", staged.name, source)
        staged.unlink()
        lap("staging")
        job.run("batch-job.pig", result, INPUT=source, PARSED=f"{job.root}/parsed/{batch_id}",
                SAMPLE_LIMIT=SAMPLE_LIMIT)
        lap("pig")

        with TemporaryDirectory(dir=job.work, prefix="parts-") as download:
            job.dfs("-get", result, download)
            local = Path(download) / f"result-{batch_id}"
            stats = list(rows(local / "stats"))
            if len(stats) != 1 or stats[0]["record_count"] != count:
                raise ValueError(f"Pig did not account for all {count} input records in batch {batch_id}")
            emit(streams["batches.jsonl"], {"batch_id": batch_id, **stats[0]})
            valid_count += count - stats[0]["malformed_count"]
            write_queries(streams, local, batch_id)
            samples = sorted(rows(local / "malformed"), key=lambda row: row["record_number"])
            for row in samples[:SAMPLE_LIMIT - samples_written]:
                emit(streams["malformed_samples.jsonl"], {"batch_id": batch_id, **row})
                samples_written += 1
            if debug:
                for row in rows(local / "debug"):
                    emit(streams["parsed_records.jsonl"], {"batch_id": batch_id, **row})
        job.dfs("-rm", "-r", "-skipTrash", source, result)
        lap("collect")

    if valid_count:
        result = f"{job.root}/whole"
        job.run("whole-job.pig", result, PARSED=f"{job.root}/parsed")
        lap("pig")
        with TemporaryDirectory(dir=job.work, prefix="parts-") as download:
            job.dfs("-get", result, download)
            write_queries(streams, Path(download) / "whole", None)
        lap("collect")


def run_adapter(inputs, batch_size, output, debug):
    if batch_size <= 0:
        raise ValueError("Batch size must be a positive integer.")
    for path in inputs:
        if any(char in path.name for char in "\t\r\n"):
            raise ValueError(f"Input basename cannot contain a tab or line break: {path.name!r}")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError(f"Output directory must be empty: {output}")
    work = output / "_work"
    work.mkdir()
    names = REQUIRED_FILES + (["parsed_records.jsonl"] if debug else [])
    timings = {}
    # Peek once so empty inputs still produce all required files without a Hadoop job.
    records = iter_records(inputs, batch_size)
    first = next(records, None)
    with ExitStack() as stack:
        stack.callback(records.close)
        streams = {name: stack.enter_context((work / name).open("x", encoding="utf-8", newline="\n"))
                   for name in names}
        if first is not None:
            job = PigJob(work, debug)
            job.dfs("-mkdir", job.root)
            try:
                job.dfs("-mkdir", f"{job.root}/tmp", f"{job.root}/parsed")
                process_batches(job, chain((first,), records), streams, debug, timings)
                job.dfs("-rm", "-r", "-skipTrash", job.root)
            except (Exception, KeyboardInterrupt):
                print(f"Pig diagnostics: {job.log}; HDFS work path: {job.root}", file=sys.stderr)
                raise
    with (work / "phase_timings.jsonl").open("x", encoding="utf-8", newline="\n") as stream:
        for phase, seconds in timings.items():
            emit(stream, {"phase_name": phase, "duration_seconds": seconds})
    for name in names + ["phase_timings.jsonl"]:
        (work / name).replace(output / name)


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=default_batch_size())
    parser.add_argument("--input", action="append", type=Path, help="Repeat in input order.")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--debug-parsed", action="store_true")
    parser.add_argument("--engine-info", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.engine_info:
            print(json.dumps(engine_info()))
        else:
            if args.output_dir is None:
                parser.error("--output-dir is required for adapter runs")
            inputs = [repo_path(path) for path in args.input] if args.input else default_input_files()
            run_adapter(inputs, args.batch_size, args.output_dir.resolve(), args.debug_parsed)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Pig adapter: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
