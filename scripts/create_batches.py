#!/usr/bin/env python3

"""
Create unchanged raw-record batches for the NASA HTTP log project.

This script performs NO parsing, cleaning, filtering, or transformation.
It only partitions original input lines into sequential batch files.
"""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_INPUTS = [
    PROJECT_ROOT / "data/raw/NASA_access_log_Jul95",
    PROJECT_ROOT / "data/raw/NASA_access_log_Aug95",
]

DEFAULT_BATCH_SIZE = 1_000_000


def create_batches(input_files, batch_size, run_id, output_root):
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero.")

    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID.")

    for path in input_files:
        if not path.is_file():
            raise FileNotFoundError(f"Input file not found: {path}")

    run_dir = output_root / run_id

    # Never silently overwrite an existing run.
    run_dir.mkdir(parents=True, exist_ok=False)

    batches = []
    total_records = 0
    current_writer = None
    current_batch_count = 0
    current_segments = []
    batch_number = 0

    def finish_batch():
        nonlocal current_writer
        nonlocal current_batch_count
        nonlocal current_segments

        if current_writer is None:
            return

        current_writer.close()

        batches.append({
            "batch_id": batch_number,
            "file": f"batch_{batch_number:04d}.log",
            "record_count": current_batch_count,
            "source_segments": current_segments,
        })

        current_writer = None
        current_batch_count = 0
        current_segments = []

    try:
        for input_path in input_files:
            source_record_number = 0

            with input_path.open("rb") as source:
                while True:
                    raw_line = source.readline()

                    if raw_line == b"":
                        break

                    source_record_number += 1

                    if current_writer is None:
                        batch_number += 1
                        batch_path = (
                            run_dir / f"batch_{batch_number:04d}.log"
                        )
                        current_writer = batch_path.open("wb")

                    # Write the original bytes unchanged.
                    current_writer.write(raw_line)

                    current_batch_count += 1
                    total_records += 1

                    # Track source-file boundaries in the batch.
                    if (
                        not current_segments
                        or current_segments[-1]["source_file"]
                        != input_path.name
                    ):
                        current_segments.append({
                            "source_file": input_path.name,
                            "first_record": source_record_number,
                            "last_record": source_record_number,
                            "records": 1,
                        })
                    else:
                        segment = current_segments[-1]
                        segment["last_record"] = source_record_number
                        segment["records"] += 1

                    if current_batch_count == batch_size:
                        finish_batch()

        # Preserve the final partial batch.
        finish_batch()

    except Exception:
        if current_writer is not None:
            current_writer.close()
        raise

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "created_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "batch_size": batch_size,
        "input_files": [p.name for p in input_files],
        "total_records": total_records,
        "total_batches": len(batches),
        "average_batch_size": (
            total_records / len(batches) if batches else 0
        ),
        "batches": batches,
    }

    manifest_path = run_dir / "manifest.json"

    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    return manifest, run_dir


def main():
    parser = argparse.ArgumentParser(
        description="Create unchanged raw NASA log batches."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
    )

    parser.add_argument(
        "--run-id",
        default=datetime.now(timezone.utc).strftime(
            "RUN_%Y%m%d_%H%M%S"
        ),
    )

    parser.add_argument(
        "--input-files",
        nargs="+",
        type=Path,
        default=DEFAULT_INPUTS,
    )

    parser.add_argument(
        "--output-root",
        type=Path,
        default=PROJECT_ROOT / "data/work",
    )

    args = parser.parse_args()

    input_files = [
        p.resolve() for p in args.input_files
    ]

    output_root = args.output_root.resolve()

    manifest, run_dir = create_batches(
        input_files=input_files,
        batch_size=args.batch_size,
        run_id=args.run_id,
        output_root=output_root,
    )

    print("\nBatch generation successful!")
    print("--------------------------------")
    print(f"Run ID          : {manifest['run_id']}")
    print(f"Batch size      : {manifest['batch_size']:,}")
    print(f"Total records   : {manifest['total_records']:,}")
    print(f"Total batches   : {manifest['total_batches']}")
    print(
        f"Average size    : "
        f"{manifest['average_batch_size']:,.2f}"
    )
    print(f"Output directory: {run_dir}")
    print(
        f"Manifest        : {run_dir / 'manifest.json'}"
    )

    print("\nIndividual batches:")

    for batch in manifest["batches"]:
        print(
            f"Batch {batch['batch_id']}: "
            f"{batch['record_count']:,} records"
        )


if __name__ == "__main__":
    main()
