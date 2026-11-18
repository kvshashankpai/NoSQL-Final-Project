#!/usr/bin/env python3
"""Diagnostic batch splitter for the NASA HTTP log project.

Writes one file per batch so batches can be inspected by hand. It is NOT part
of the production path: each adapter assigns batches with the shared staging
stage (etl_common/staging.py), which this script also uses, so the record
numbers and batch boundaries here are the same as in every pipeline.

Batch file format: each record's content followed by a single LF. Line
terminators are normalised (a CRLF terminator becomes LF), so any CR left in
a batch file is record content, for example a bare CR at the end of a file's
unterminated last line. Read batch files by splitting on LF only, and never
strip a CR. The bytes of each record are otherwise unchanged (no decoding).

This script performs NO parsing, cleaning, filtering or field conversion.

Output directory: <output-root>/<run-id>/
  _INCOMPLETE        present while writing; left behind if the run fails
  batch_NNNN.log     one per non-empty batch
  manifest.json      status "complete", or "failed" with the error

Defaults (input files, batch size, output root) come from
config/project_config.json.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl_common.config import default_batch_size, default_input_files, load_config, repo_path  # noqa: E402
from etl_common.staging import batch_id_for, iter_raw_lines  # noqa: E402

INCOMPLETE_MARKER = "_INCOMPLETE"


def _write_json_atomic(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def create_batches(input_files, batch_size, run_id, output_root):
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero.")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID.")
    for path in input_files:
        if not Path(path).is_file():
            raise FileNotFoundError(f"Input file not found: {path}")

    run_dir = Path(output_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=False)  # never overwrite an existing run
    (run_dir / INCOMPLETE_MARKER).write_text(
        "Batch generation did not finish. Do not use these files.\n", encoding="utf-8")

    manifest = {
        "schema_version": 2,
        "status": "running",
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_size": batch_size,
        "input_files": [Path(p).name for p in input_files],
        "batch_file_format": "record content + LF; split on LF only; CR is content",
    }
    batches, writer, current = [], None, None
    try:
        for record_number, source, line in iter_raw_lines(input_files):
            batch_id = batch_id_for(record_number, batch_size)
            if batch_id != current:
                if writer:
                    writer.close()
                current = batch_id
                file_name = f"batch_{batch_id:04d}.log"
                writer = open(run_dir / file_name, "wb")
                batches.append({"batch_id": batch_id, "file": file_name,
                                "record_count": 0, "source_segments": []})
            writer.write(line + b"\n")
            batch = batches[-1]
            batch["record_count"] += 1
            segs = batch["source_segments"]
            if segs and segs[-1]["source_file"] == source:
                segs[-1]["last_record"] = record_number
                segs[-1]["records"] += 1
            else:
                segs.append({"source_file": source, "first_record": record_number,
                             "last_record": record_number, "records": 1})
        if writer:
            writer.close()
            writer = None
    except BaseException as exc:
        if writer:
            writer.close()
        manifest.update(status="failed", error=f"{type(exc).__name__}: {exc}",
                        batches_written=len(batches))
        _write_json_atomic(run_dir / "manifest.json", manifest)
        raise

    total = sum(b["record_count"] for b in batches)
    manifest.update(
        status="complete",
        total_records=total,
        total_batches=len(batches),
        # NULL (JSON null) when there are no non-empty batches.
        average_batch_size=total / len(batches) if batches else None,
        batches=batches,
    )
    _write_json_atomic(run_dir / "manifest.json", manifest)
    (run_dir / INCOMPLETE_MARKER).unlink()
    return manifest, run_dir


def main(argv=None):
    config = load_config()
    parser = argparse.ArgumentParser(description="Write diagnostic batch files (not used in production).")
    parser.add_argument("--batch-size", type=int, default=default_batch_size(config))
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("RUN_%Y%m%d_%H%M%S"))
    parser.add_argument("--input-files", nargs="+", type=Path, default=default_input_files(config))
    parser.add_argument("--output-root", type=Path,
                        default=repo_path(config["batching"]["temporary_directory"]))
    args = parser.parse_args(argv)

    manifest, run_dir = create_batches(
        input_files=[repo_path(p).resolve() for p in args.input_files],
        batch_size=args.batch_size,
        run_id=args.run_id,
        output_root=repo_path(args.output_root).resolve(),
    )
    avg = manifest["average_batch_size"]
    print("\nBatch generation successful!")
    print("--------------------------------")
    print(f"Run ID          : {manifest['run_id']}")
    print(f"Batch size      : {manifest['batch_size']:,}")
    print(f"Total records   : {manifest['total_records']:,}")
    print(f"Total batches   : {manifest['total_batches']}")
    print(f"Average size    : {'NULL (no batches)' if avg is None else f'{avg:,.2f}'}")
    print(f"Output directory: {run_dir}")
    for batch in manifest["batches"]:
        print(f"Batch {batch['batch_id']}: {batch['record_count']:,} records")


if __name__ == "__main__":
    main()
