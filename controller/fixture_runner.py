"""Run one adapter on every test fixture and compare with the expected outputs.

    python -m controller.fixture_runner --pipeline Pig [--fixture parsing ...]

No database is involved. For each fixture: run the adapter with
--debug-parsed into a temporary directory, validate the output, then compare
every file with tests/fixtures/<name>/expected in both directions.
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from controller.compare import compare_dirs, format_report
from controller.adapters import launcher
from controller.validation import validate_output
from etl_common.config import REPO_ROOT

FIXTURES = REPO_ROOT / "tests" / "fixtures"


def fixture_names():
    return sorted(p.parent.name for p in FIXTURES.glob("*/fixture.json"))


def run_fixture(pipeline, name, work_dir):
    fx = FIXTURES / name
    meta = json.loads((fx / "fixture.json").read_text(encoding="utf-8"))
    out = Path(work_dir) / name
    cmd = launcher(pipeline) + ["--batch-size", str(meta["batch_size"]), "--output-dir", str(out),
                                 "--debug-parsed"]
    for rel in meta["inputs"]:
        cmd += ["--input", str(fx / rel)]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        return False, f"adapter failed ({proc.returncode}):\n{proc.stderr[-3000:]}"
    res = validate_output(out, meta["batch_size"], require_parsed=True)
    if not res.ok:
        return False, "validation errors:\n  " + "\n  ".join(res.errors[:30])
    diffs = compare_dirs(fx / "expected", out)
    if all(d.equal for d in diffs):
        return True, "equivalent"
    return False, format_report(diffs)


def run_fixtures(pipeline, names=None, log=print):
    ok_all = True
    with tempfile.TemporaryDirectory(prefix="fixtures_") as tmp:
        for name in names or fixture_names():
            ok, detail = run_fixture(pipeline, name, tmp)
            ok_all &= ok
            log(f"[{'PASS' if ok else 'FAIL'}] {pipeline} / {name}: {detail if not ok else 'equivalent'}")
    return ok_all


def main(argv=None):
    ap = argparse.ArgumentParser(description="Run an adapter on the test fixtures and compare outputs.")
    ap.add_argument("--pipeline", required=True, help="Pig, MapReduce, MongoDB or Hive")
    ap.add_argument("--fixture", action="append", default=None, help="fixture name (default: all)")
    args = ap.parse_args(argv)
    return 0 if run_fixtures(args.pipeline, args.fixture) else 1


if __name__ == "__main__":
    sys.exit(main())
