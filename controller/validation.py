"""Validate one adapter output directory against the output contract.

Used by the controller before loading (a run with any error is not loaded),
and from the command line:

    python -m controller.validation OUTPUT_DIR --batch-size N [--require-parsed]

Checks: required files, exact fields and types, duplicate keys, consecutive
batch IDs and batch sizes, count consistency between files and scopes,
top-20 ranks and ordering, error rates, and malformed samples.
"""

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from controller.outputs import (KEYS, OPTIONAL_FILES, RATE_TOLERANCE, REQUIRED_FILES,
                                SAMPLE_LIMIT, SCHEMAS, TOP_N, read_jsonl)


@dataclass
class ValidationResult:
    errors: list = field(default_factory=list)
    data: dict = field(default_factory=dict)  # file name -> rows (only well-formed rows)

    @property
    def ok(self):
        return not self.errors

    @property
    def totals(self):
        batches = self.data.get("batches.jsonl", [])
        records = sum(b["record_count"] for b in batches)
        return {"total_records": records,
                "total_batches": len(batches),
                "malformed_count": sum(b["malformed_count"] for b in batches),
                "average_batch_size": records / len(batches) if batches else None}


def validate_output(out_dir, batch_size, require_parsed=False, max_errors=200):
    out_dir = Path(out_dir)
    res = ValidationResult()
    err = res.errors.append

    if not out_dir.is_dir():
        err(f"output directory does not exist: {out_dir}")
        return res

    allowed = set(REQUIRED_FILES) | set(OPTIONAL_FILES)
    for entry in sorted(out_dir.iterdir()):
        if not entry.name.startswith("_") and entry.name not in allowed:
            err(f"unexpected entry in output directory: {entry.name}")
    wanted = REQUIRED_FILES + [f for f in OPTIONAL_FILES
                               if (out_dir / f).exists() or (f == "parsed_records.jsonl" and require_parsed)]
    for name in wanted:
        path = out_dir / name
        if not path.is_file():
            err(f"missing required file: {name}")
            continue
        rows, parse_errors = read_jsonl(path)
        res.errors.extend(parse_errors)
        res.data[name] = _check_rows(name, rows, err)
    if res.errors:
        return _cap(res, max_errors)

    _check_batches(res.data, batch_size, err)
    _check_scopes(res.data, err)
    _check_daily(res.data, err)
    _check_top(res.data, err)
    _check_hourly(res.data, err)
    _check_samples(res.data, batch_size, err)
    if "parsed_records.jsonl" in res.data:
        _check_parsed(res.data, batch_size, err)
    return _cap(res, max_errors)


def _cap(res, max_errors):
    if len(res.errors) > max_errors:
        extra = len(res.errors) - max_errors
        res.errors = res.errors[:max_errors] + [f"... and {extra} more errors"]
    return res


def _check_rows(name, rows, err):
    schema, key_of = SCHEMAS[name], KEYS[name]
    good, seen = [], set()
    for n, row in enumerate(rows, 1):
        if set(row) != set(schema):
            missing = sorted(set(schema) - set(row))
            extra = sorted(set(row) - set(schema))
            err(f"{name}:{n}: wrong fields (missing {missing}, extra {extra})")
            continue
        bad = [f for f, check in schema.items() if not check(row[f])]
        if bad:
            err(f"{name}:{n}: invalid value or type for {bad}: "
                + ", ".join(f"{f}={row[f]!r}" for f in bad))
            continue
        key = key_of(row)
        if key in seen:
            err(f"{name}:{n}: duplicate key {key}")
            continue
        seen.add(key)
        good.append(row)
    return good


def _check_batches(data, batch_size, err):
    batches = sorted(data["batches.jsonl"], key=lambda b: b["batch_id"])
    ids = [b["batch_id"] for b in batches]
    if ids != list(range(1, len(ids) + 1)):
        err(f"batches.jsonl: batch IDs must be 1..n with no gaps, got {ids[:10]}{'...' if len(ids) > 10 else ''}")
    for i, b in enumerate(batches):
        last = i == len(batches) - 1
        if b["malformed_count"] > b["record_count"]:
            err(f"batches.jsonl: batch {b['batch_id']} has more malformed than records")
        if not last and b["record_count"] != batch_size:
            err(f"batches.jsonl: batch {b['batch_id']} has {b['record_count']} records; "
                f"every batch but the last must have exactly {batch_size}")
        if last and b["record_count"] > batch_size:
            err(f"batches.jsonl: last batch has {b['record_count']} records, more than {batch_size}")


def _check_scopes(data, err):
    ids = {b["batch_id"] for b in data["batches.jsonl"]}
    for name in ("daily_traffic.jsonl", "top_resources.jsonl", "hourly_errors.jsonl"):
        for r in data[name]:
            if r["batch_id"] is not None and r["batch_id"] not in ids:
                err(f"{name}: row refers to batch {r['batch_id']}, which is not in batches.jsonl")


def _valid_counts(data):
    """Valid records per batch, from batches.jsonl."""
    return {b["batch_id"]: b["record_count"] - b["malformed_count"] for b in data["batches.jsonl"]}


def _check_daily(data, err):
    rows = data["daily_traffic.jsonl"]
    valid = _valid_counts(data)
    per_batch, whole = defaultdict(lambda: [0, 0]), {}
    count_by_scope = defaultdict(int)
    for r in rows:
        count_by_scope[r["batch_id"]] += r["request_count"]
        k = (r["log_date"], r["status_code"])
        if r["batch_id"] is None:
            whole[k] = (r["request_count"], r["total_bytes"])
        else:
            per_batch[k][0] += r["request_count"]
            per_batch[k][1] += r["total_bytes"]
    for b, n in valid.items():
        if count_by_scope.get(b, 0) != n:
            err(f"daily_traffic.jsonl: batch {b} counts {count_by_scope.get(b, 0)} requests "
                f"but batches.jsonl has {n} valid records")
    if count_by_scope.get(None, 0) != sum(valid.values()):
        err(f"daily_traffic.jsonl: whole run counts {count_by_scope.get(None, 0)} requests "
            f"but batches.jsonl has {sum(valid.values())} valid records")
    if {k: tuple(v) for k, v in per_batch.items()} != whole:
        err("daily_traffic.jsonl: whole-run rows are not the sum of the per-batch rows")


def _check_top(data, err):
    by_scope = defaultdict(list)
    for r in data["top_resources.jsonl"]:
        by_scope[r["batch_id"]].append(r)
        if r["distinct_host_count"] > r["request_count"]:
            err(f"top_resources.jsonl: {r['resource_path']!r} has more distinct hosts than requests")
    for scope, rows in by_scope.items():
        label = "whole run" if scope is None else f"batch {scope}"
        ranks = sorted(r["rank"] for r in rows)
        if ranks != list(range(1, len(rows) + 1)):
            err(f"top_resources.jsonl: {label}: ranks must be 1..{len(rows)}, got {ranks}")
            continue
        if len(rows) > TOP_N:
            err(f"top_resources.jsonl: {label}: more than {TOP_N} rows")
        by_rank = sorted(rows, key=lambda r: r["rank"])
        expected = sorted(rows, key=lambda r: (-r["request_count"], r["resource_path"].encode("utf-8")))
        if [r["resource_path"] for r in by_rank] != [r["resource_path"] for r in expected]:
            err(f"top_resources.jsonl: {label}: ranks do not follow request count descending, "
                f"then binary UTF-8 path order")
    # A path counted in the whole run must have at least the requests of any one batch.
    whole = {r["resource_path"]: r for r in by_scope.get(None, [])}
    for scope, rows in by_scope.items():
        if scope is None:
            continue
        for r in rows:
            w = whole.get(r["resource_path"])
            if w and (w["request_count"] < r["request_count"]
                      or w["distinct_host_count"] < r["distinct_host_count"]):
                err(f"top_resources.jsonl: whole-run row for {r['resource_path']!r} is smaller "
                    f"than batch {scope}'s row")
    if any(v > 0 for v in _valid_counts(data).values()) and not by_scope.get(None) \
            and any(by_scope.get(b) for b in by_scope if b is not None):
        err("top_resources.jsonl: per-batch rows exist but there are no whole-run rows")


def _check_hourly(data, err):
    rows = data["hourly_errors.jsonl"]
    valid = _valid_counts(data)
    total_by_scope = defaultdict(int)
    per_batch, whole = defaultdict(lambda: [0, 0, 0, 0]), {}
    for r in rows:
        e, t, d = r["error_request_count"], r["total_request_count"], r["distinct_error_hosts"]
        where = f"hourly_errors.jsonl: {r['log_date']} hour {r['log_hour']} batch {r['batch_id']}"
        if e > t:
            err(f"{where}: more errors than requests")
        if d > e or (e == 0) != (d == 0):
            err(f"{where}: distinct error hosts {d} inconsistent with error count {e}")
        if abs(r["error_rate"] - e / t) > RATE_TOLERANCE:
            err(f"{where}: error_rate {r['error_rate']} differs from {e}/{t} by more than {RATE_TOLERANCE}")
        total_by_scope[r["batch_id"]] += t
        k = (r["log_date"], r["log_hour"])
        if r["batch_id"] is None:
            whole[k] = (e, t, d)
        else:
            a = per_batch[k]
            a[0] += e
            a[1] += t
            a[2] = max(a[2], d)
            a[3] += d
    for b, n in valid.items():
        if total_by_scope.get(b, 0) != n:
            err(f"hourly_errors.jsonl: batch {b} counts {total_by_scope.get(b, 0)} requests "
                f"but batches.jsonl has {n} valid records")
    if total_by_scope.get(None, 0) != sum(valid.values()):
        err("hourly_errors.jsonl: whole-run request total does not match the valid records")
    if set(per_batch) != set(whole):
        err("hourly_errors.jsonl: whole-run and per-batch rows cover different date/hours")
    for k, (e, t, dmax, dsum) in per_batch.items():
        if k in whole:
            we, wt, wd = whole[k]
            if (we, wt) != (e, t):
                err(f"hourly_errors.jsonl: whole-run counts for {k} are not the sum of the batches")
            if not dmax <= wd <= dsum:
                err(f"hourly_errors.jsonl: whole-run distinct error hosts for {k} is {wd}, "
                    f"outside the possible range {dmax}..{dsum}")


def _check_samples(data, batch_size, err):
    samples = data["malformed_samples.jsonl"]
    total_malformed = sum(b["malformed_count"] for b in data["batches.jsonl"])
    total_records = sum(b["record_count"] for b in data["batches.jsonl"])
    expected = min(SAMPLE_LIMIT, total_malformed)
    if len(samples) != expected:
        err(f"malformed_samples.jsonl: has {len(samples)} rows, expected {expected}")
    numbers = [s["record_number"] for s in samples]
    if numbers != sorted(numbers):
        err("malformed_samples.jsonl: rows are not in input order")
    per_batch = defaultdict(int)
    for s in samples:
        per_batch[s["batch_id"]] += 1
        if s["record_number"] > total_records:
            err(f"malformed_samples.jsonl: record {s['record_number']} is beyond the last record")
        if s["batch_id"] != (s["record_number"] - 1) // batch_size + 1:
            err(f"malformed_samples.jsonl: record {s['record_number']} has wrong batch {s['batch_id']}")
        if (s["reason"] == "BLANK_LINE") != (s["raw_line"] == ""):
            err(f"malformed_samples.jsonl: record {s['record_number']}: BLANK_LINE must match an empty raw_line")
        if (s["reason"] == "NUL_BYTE") != ("\x00" in s["raw_line"]):
            err(f"malformed_samples.jsonl: record {s['record_number']}: NUL_BYTE must match a NUL in raw_line")
    malformed = {b["batch_id"]: b["malformed_count"] for b in data["batches.jsonl"]}
    for b, n in per_batch.items():
        if n > malformed.get(b, 0):
            err(f"malformed_samples.jsonl: batch {b} has more samples than malformed records")
    if total_malformed <= SAMPLE_LIMIT:
        for b, n in malformed.items():
            if per_batch.get(b, 0) != n:
                err(f"malformed_samples.jsonl: batch {b} should have {n} samples, has {per_batch.get(b, 0)}")


def _check_parsed(data, batch_size, err):
    parsed = sorted(data["parsed_records.jsonl"], key=lambda r: r["record_number"])
    total = sum(b["record_count"] for b in data["batches.jsonl"])
    if [r["record_number"] for r in parsed] != list(range(1, total + 1)):
        err(f"parsed_records.jsonl: record numbers must be 1..{total}")
    fields = ("host", "timestamp", "log_date", "log_hour", "http_method", "resource_path",
              "protocol_version", "status_code", "bytes_transferred")
    required = ("host", "timestamp", "log_date", "log_hour", "status_code", "bytes_transferred")
    malformed = defaultdict(int)
    for r in parsed:
        n = r["record_number"]
        if r["batch_id"] != (n - 1) // batch_size + 1:
            err(f"parsed_records.jsonl: record {n} has wrong batch {r['batch_id']}")
        if r["valid"]:
            if r["reason"] is not None or any(r[f] is None for f in required):
                err(f"parsed_records.jsonl: valid record {n} has a reason or a missing required field")
        else:
            malformed[r["batch_id"]] += 1
            if r["reason"] is None or any(r[f] is not None for f in fields):
                err(f"parsed_records.jsonl: malformed record {n} needs a reason and all fields null")
    for b in data["batches.jsonl"]:
        if malformed.get(b["batch_id"], 0) != b["malformed_count"]:
            err(f"parsed_records.jsonl: batch {b['batch_id']} malformed count disagrees with batches.jsonl")
    samples = {s["record_number"]: s for s in data["malformed_samples.jsonl"]}
    by_number = {r["record_number"]: r for r in parsed}
    for n, s in samples.items():
        r = by_number.get(n)
        if r is None or r["valid"] or r["reason"] != s["reason"] or r["source_file"] != s["source_file"]:
            err(f"malformed_samples.jsonl: record {n} disagrees with parsed_records.jsonl")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Validate an adapter output directory.")
    ap.add_argument("output_dir", type=Path)
    ap.add_argument("--batch-size", type=int, required=True)
    ap.add_argument("--require-parsed", action="store_true",
                    help="also require parsed_records.jsonl (fixture runs with --debug-parsed)")
    args = ap.parse_args(argv)
    res = validate_output(args.output_dir, args.batch_size, args.require_parsed)
    for e in res.errors:
        print("ERROR:", e)
    if res.ok:
        t = res.totals
        print(f"OK: {t['total_records']} records, {t['total_batches']} batches, "
              f"{t['malformed_count']} malformed")
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(main())
