#!/usr/bin/env python3
"""Record the input files' order, sizes and SHA-256 checksums.

Run once per benchmark session, before any timed run (EXPERIMENT_DESIGN.md
section 3). Hashing reads the whole dataset, so it is never part of a
measured run.

    python scripts/verify_inputs.py            # inputs from the config
    python scripts/verify_inputs.py --input a --input b

Writes verification/<UTC timestamp>.json and verification/latest.json.
The controller refuses to start a run unless latest.json lists the same
files, in the same order, with the same sizes and modification times. It
copies the record into runs.input_files for every run.
"""

import argparse
import hashlib
import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl_common.config import REPO_ROOT, default_input_files, load_config, repo_path  # noqa: E402

VERIFICATION_DIR = REPO_ROOT / "verification"
CHUNK = 8 * 1024 * 1024


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def describe(path):
    st = os.stat(path)
    return {"name": Path(path).name,
            "path": Path(path).resolve().relative_to(REPO_ROOT).as_posix()
            if Path(path).resolve().is_relative_to(REPO_ROOT) else str(Path(path).resolve()),
            "size_bytes": st.st_size,
            "mtime_ns": st.st_mtime_ns}


def build_record(input_files, encoding):
    files = []
    for order, path in enumerate(input_files, 1):
        before = describe(path)
        checksum = sha256_of(path)
        after = describe(path)
        if before != after:
            raise ValueError(f"Input changed while hashing: {path}; repeat verification")
        entry = {"order": order, **after, "sha256": checksum}
        files.append(entry)
    return {"schema_version": 1,
            "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            "encoding": encoding,
            "host": platform.node(),
            "files": files}


def write_record(record, out_dir=VERIFICATION_DIR):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(record["verified_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{stamp}-{uuid4().hex[:12]}.json"
    text = json.dumps(record, indent=2) + "\n"
    for target in (path, out_dir / "latest.json"):
        temporary = out_dir / f".v-{uuid4().hex[:16]}.tmp"
        try:
            with temporary.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    return path


def check_current(record, input_files):
    """Cheap pre-run check (stat only, no reading): return a list of mismatches."""
    problems = []
    if len(record["files"]) != len(input_files):
        return [f"verification lists {len(record['files'])} files, run has {len(input_files)}"]
    for entry, path in zip(record["files"], input_files):
        if repo_path(entry["path"]).resolve() != Path(path).resolve():
            problems.append(f"path mismatch: verified {entry['path']}, run has {path}")
            continue
        if entry["name"] != Path(path).name:
            problems.append(f"order/name mismatch: verified {entry['name']}, run has {Path(path).name}")
            continue
        if not Path(path).is_file():
            problems.append(f"missing input file: {path}")
            continue
        st = os.stat(path)
        if st.st_size != entry["size_bytes"] or st.st_mtime_ns != entry["mtime_ns"]:
            problems.append(f"{entry['name']} changed since verification (size or modification time); "
                            f"run scripts/verify_inputs.py again")
    return problems


def load_latest(out_dir=VERIFICATION_DIR):
    path = Path(out_dir) / "latest.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv=None):
    config = load_config()
    ap = argparse.ArgumentParser(description="Record input file order, sizes and SHA-256 checksums.")
    ap.add_argument("--input", action="append", type=Path,
                    help="input file, repeat in processing order (default: config input_files)")
    ap.add_argument("--out-dir", type=Path, default=VERIFICATION_DIR)
    args = ap.parse_args(argv)
    inputs = [repo_path(p) for p in args.input] if args.input else default_input_files(config)
    missing = [str(p) for p in inputs if not Path(p).is_file()]
    if missing:
        print("Input files not found:\n  " + "\n  ".join(missing), file=sys.stderr)
        return 2
    record = build_record(inputs, config["dataset"]["encoding"])
    path = write_record(record, args.out_dir)
    for f in record["files"]:
        print(f"{f['order']}. {f['name']}  {f['size_bytes']:,} bytes  sha256 {f['sha256']}")
    print(f"Saved {path} and latest.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
