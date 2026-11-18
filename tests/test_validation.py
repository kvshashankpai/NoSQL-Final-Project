"""The validator and comparator must catch broken outputs."""

import json
import shutil
from pathlib import Path

import pytest

from controller.compare import compare_dirs
from controller.validation import validate_output

FIX = Path(__file__).resolve().parent / "fixtures"


def copy_expected(tmp_path, name="query"):
    dst = tmp_path / name
    shutil.copytree(FIX / name / "expected", dst)
    return dst


def rewrite(path, fn):
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows = fn(rows)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8", newline="\n")


def errors_after(tmp_path, file, fn, batch_size=3, name="query"):
    d = copy_expected(tmp_path, name)
    rewrite(d / file, fn)
    return validate_output(d, batch_size).errors


def test_expected_query_outputs_are_valid():
    assert validate_output(FIX / "query" / "expected", 3, require_parsed=True).errors == []


def test_missing_file(tmp_path):
    d = copy_expected(tmp_path)
    (d / "hourly_errors.jsonl").unlink()
    assert any("missing required file: hourly_errors.jsonl" in e for e in validate_output(d, 3).errors)


def test_unexpected_file_and_ignored_work_dir(tmp_path):
    d = copy_expected(tmp_path)
    (d / "_work").mkdir()
    (d / "notes.txt").write_text("x")
    errors = validate_output(d, 3).errors
    assert errors == ["unexpected entry in output directory: notes.txt"]


@pytest.mark.parametrize("file,fn,needle", [
    ("daily_traffic.jsonl", lambda rs: [{**rs[0], "extra": 1}] + rs[1:], "wrong fields"),
    ("daily_traffic.jsonl", lambda rs: [{**rs[0], "status_code": "200"}] + rs[1:], "invalid value or type"),
    ("daily_traffic.jsonl", lambda rs: [{**rs[0], "request_count": True}] + rs[1:], "invalid value or type"),
    ("daily_traffic.jsonl", lambda rs: [{**rs[0], "log_date": "1995-02-30"}] + rs[1:], "invalid value or type"),
    ("daily_traffic.jsonl", lambda rs: rs + [rs[0]], "duplicate key"),
    ("daily_traffic.jsonl", lambda rs: [{**rs[0], "request_count": rs[0]["request_count"] + 1}] + rs[1:],
     "counts"),
    ("daily_traffic.jsonl", lambda rs: [{**rs[0], "batch_id": 9}] + rs[1:], "not in batches.jsonl"),
    ("batches.jsonl", lambda rs: [rs[0], {**rs[2], "batch_id": 4}], "no gaps"),
    ("batches.jsonl", lambda rs: [{**rs[0], "record_count": 2}] + rs[1:], "exactly 3"),
    ("top_resources.jsonl", lambda rs: [{**r, "rank": 3 - r["rank"]} if r["batch_id"] is None else r
                                        for r in rs], "binary UTF-8"),
    ("top_resources.jsonl", lambda rs: [{**r, "rank": r["rank"] + 1} if r["batch_id"] is None else r
                                        for r in rs], "ranks must be"),
    ("hourly_errors.jsonl", lambda rs: [{**rs[0], "error_rate": 0.34}] + rs[1:], "error_rate"),
    ("hourly_errors.jsonl", lambda rs: [{**r, "distinct_error_hosts": 3} if r["batch_id"] is None
                                        and r["log_hour"] == 0 else r for r in rs], "distinct error hosts"),
    ("malformed_samples.jsonl", lambda rs: [], "expected 1"),
    ("malformed_samples.jsonl", lambda rs: [{**rs[0], "reason": "OOPS"}], "invalid value"),
    ("malformed_samples.jsonl", lambda rs: [{**rs[0], "batch_id": 1}], "wrong batch"),
])
def test_validator_catches(tmp_path, file, fn, needle):
    errors = errors_after(tmp_path, file, fn)
    assert any(needle in e for e in errors), errors


def test_invalid_json_and_nan(tmp_path):
    d = copy_expected(tmp_path)
    with open(d / "hourly_errors.jsonl", "a", encoding="utf-8", newline="\n") as f:
        f.write('{"batch_id": null, "error_rate": NaN}\n{not json}\n')
    errors = validate_output(d, 3).errors
    assert sum("invalid JSON" in e for e in errors) == 2


def test_comparator_both_directions(tmp_path):
    d = copy_expected(tmp_path)
    rewrite(d / "daily_traffic.jsonl", lambda rs: rs[1:] + [{**rs[0], "log_date": "1999-01-01"}])
    rewrite(d / "hourly_errors.jsonl", lambda rs: [{**rs[0], "error_rate": rs[0]["error_rate"] + 5e-10}] + rs[1:])
    rewrite(d / "top_resources.jsonl", lambda rs: [{**rs[0], "total_bytes": 1}] + rs[1:])
    diffs = {x.name: x for x in compare_dirs(FIX / "query" / "expected", d)}
    q1 = diffs["daily_traffic.jsonl"]
    assert len(q1.missing) == 1 and len(q1.extra) == 1
    assert diffs["hourly_errors.jsonl"].equal          # within 1e-9
    assert len(diffs["top_resources.jsonl"].changed) == 1
    assert diffs["batches.jsonl"].equal


def test_comparator_rejects_int_float_confusion(tmp_path):
    d = copy_expected(tmp_path)
    rewrite(d / "batches.jsonl", lambda rs: [{**rs[0], "record_count": 3.0}] + rs[1:])
    diffs = {x.name: x for x in compare_dirs(FIX / "query" / "expected", d)}
    assert not diffs["batches.jsonl"].equal
