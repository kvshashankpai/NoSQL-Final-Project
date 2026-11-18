"""Compare two output directories row by row, in both directions.

    python -m controller.compare EXPECTED_DIR ACTUAL_DIR

Reports, for every output file, rows missing from ACTUAL, extra rows in
ACTUAL, and rows whose values differ. Error rates are compared with an
absolute tolerance of 1e-9; everything else must be equal. Exit code 0
means the directories are equivalent.

Use it to check an adapter against a fixture's expected outputs, or to
compare two pipelines' runs on the same input.
"""

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

from controller.outputs import KEYS, OPTIONAL_FILES, RATE_TOLERANCE, REQUIRED_FILES, read_jsonl, sort_key

# Files compared by default. phase_timings is diagnostic and differs per engine.
COMPARED = REQUIRED_FILES + ["parsed_records.jsonl"]


@dataclass
class FileDiff:
    name: str
    missing: list = field(default_factory=list)   # keys in expected, not in actual
    extra: list = field(default_factory=list)     # keys in actual, not in expected
    changed: list = field(default_factory=list)   # (key, {field: (expected, actual)})
    problems: list = field(default_factory=list)  # unreadable files etc.

    @property
    def equal(self):
        return not (self.missing or self.extra or self.changed or self.problems)


def _values_equal(fname, a, b):
    if fname == "error_rate":
        return isinstance(a, (int, float)) and isinstance(b, (int, float)) and abs(a - b) <= RATE_TOLERANCE
    # Do not treat True == 1 or 1 == 1.0 as equal: types are part of the contract.
    return type(a) is type(b) and a == b


def compare_file(name, expected_dir, actual_dir):
    diff = FileDiff(name)
    rows = {}
    for side, d in (("expected", expected_dir), ("actual", actual_dir)):
        path = Path(d) / name
        if not path.is_file():
            if name in OPTIONAL_FILES:
                rows[side] = None
                continue
            diff.problems.append(f"{side} file missing: {path}")
            return diff
        data, errors = read_jsonl(path)
        diff.problems.extend(f"{side}: {e}" for e in errors)
        keyed = {}
        for r in data:
            try:
                k = KEYS[name](r)
            except KeyError as e:
                diff.problems.append(f"{side}: row without key field {e}")
                continue
            if k in keyed:
                diff.problems.append(f"{side}: duplicate key {k}")
            keyed[k] = r
        rows[side] = keyed
    if rows.get("expected") is None or rows.get("actual") is None:
        if (rows.get("expected") is None) != (rows.get("actual") is None):
            diff.problems.append("present on one side only")
        return diff
    exp, act = rows["expected"], rows["actual"]
    diff.missing = sorted((k for k in exp if k not in act), key=sort_key)
    diff.extra = sorted((k for k in act if k not in exp), key=sort_key)
    for k in sorted((k for k in exp if k in act), key=sort_key):
        e, a = exp[k], act[k]
        fields = {f: (e.get(f), a.get(f)) for f in set(e) | set(a)
                  if f not in e or f not in a or not _values_equal(f, e[f], a[f])}
        if fields:
            diff.changed.append((k, fields))
    return diff


def compare_dirs(expected_dir, actual_dir, files=None):
    return [compare_file(n, expected_dir, actual_dir) for n in (files or COMPARED)]


def format_report(diffs, limit=20):
    lines = []
    for d in diffs:
        if d.equal:
            lines.append(f"  {d.name}: equal")
            continue
        lines.append(f"  {d.name}: {len(d.missing)} missing, {len(d.extra)} extra, "
                     f"{len(d.changed)} changed, {len(d.problems)} problems")
        for p in d.problems[:limit]:
            lines.append(f"      problem: {p}")
        for k in d.missing[:limit]:
            lines.append(f"      missing: {k}")
        for k in d.extra[:limit]:
            lines.append(f"      extra:   {k}")
        for k, fields in d.changed[:limit]:
            detail = "; ".join(f"{f}: expected {e!r}, got {a!r}" for f, (e, a) in sorted(fields.items()))
            lines.append(f"      changed: {k}: {detail}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Compare two adapter output directories.")
    ap.add_argument("expected_dir", type=Path)
    ap.add_argument("actual_dir", type=Path)
    ap.add_argument("--skip-parsed", action="store_true", help="do not compare parsed_records.jsonl")
    args = ap.parse_args(argv)
    files = [f for f in COMPARED if not (args.skip_parsed and f == "parsed_records.jsonl")]
    diffs = compare_dirs(args.expected_dir, args.actual_dir, files)
    print(format_report(diffs))
    same = all(d.equal for d in diffs)
    print("EQUIVALENT" if same else "DIFFERENT")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
