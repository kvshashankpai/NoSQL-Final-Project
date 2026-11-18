"""The adapter output contract (docs/COMMON_DATA_MODEL.md) as data: files, fields, types, keys."""

import json
import math
import re
from datetime import date
from pathlib import Path

REQUIRED_FILES = ["batches.jsonl", "daily_traffic.jsonl", "top_resources.jsonl",
                  "hourly_errors.jsonl", "malformed_samples.jsonl"]
OPTIONAL_FILES = ["phase_timings.jsonl", "parsed_records.jsonl"]
REASONS = ("BLANK_LINE", "NUL_BYTE", "BAD_STRUCTURE", "BAD_HOST", "BAD_TIMESTAMP",
           "BAD_STATUS", "BAD_BYTES")
TOP_N = 20
SAMPLE_LIMIT = 100
RATE_TOLERANCE = 1e-9
MAX_BIGINT = 2**63 - 1


def is_int(v):
    return isinstance(v, int) and not isinstance(v, bool) and -MAX_BIGINT - 1 <= v <= MAX_BIGINT


def is_number(v):
    return (is_int(v) or isinstance(v, float)) and not isinstance(v, bool) and math.isfinite(v)


def is_date(v):
    if not isinstance(v, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return False
    try:
        date.fromisoformat(v)
        return True
    except ValueError:
        return False


def nonneg_int(v):
    return is_int(v) and v >= 0


def pos_int(v):
    return is_int(v) and v > 0


def opt_batch(v):
    return v is None or pos_int(v)


def text(v):
    return isinstance(v, str)


def nonempty_text(v):
    return isinstance(v, str) and v != ""


def opt_text(v):
    return v is None or isinstance(v, str)


# file -> ordered {field: type check}
SCHEMAS = {
    "batches.jsonl": {"batch_id": pos_int, "record_count": pos_int, "malformed_count": nonneg_int},
    "daily_traffic.jsonl": {"batch_id": opt_batch, "log_date": is_date,
                            "status_code": lambda v: is_int(v) and 0 <= v <= 999,
                            "request_count": pos_int, "total_bytes": nonneg_int},
    "top_resources.jsonl": {"batch_id": opt_batch, "rank": lambda v: is_int(v) and 1 <= v <= TOP_N,
                            "resource_path": nonempty_text, "request_count": pos_int,
                            "total_bytes": nonneg_int, "distinct_host_count": pos_int},
    "hourly_errors.jsonl": {"batch_id": opt_batch, "log_date": is_date,
                            "log_hour": lambda v: is_int(v) and 0 <= v <= 23,
                            "error_request_count": nonneg_int, "total_request_count": pos_int,
                            "error_rate": lambda v: is_number(v) and 0 <= v <= 1,
                            "distinct_error_hosts": nonneg_int},
    "malformed_samples.jsonl": {"source_file": nonempty_text, "record_number": pos_int,
                                "batch_id": pos_int, "reason": lambda v: v in REASONS,
                                "raw_line": text},
    "phase_timings.jsonl": {"phase_name": nonempty_text,
                            "duration_seconds": lambda v: is_number(v) and v >= 0},
    "parsed_records.jsonl": {"record_number": pos_int, "batch_id": pos_int,
                             "source_file": nonempty_text, "valid": lambda v: isinstance(v, bool),
                             "reason": lambda v: v is None or v in REASONS,
                             "host": opt_text, "timestamp": opt_text,
                             "log_date": lambda v: v is None or is_date(v),
                             "log_hour": lambda v: v is None or (is_int(v) and 0 <= v <= 23),
                             "http_method": opt_text, "resource_path": opt_text,
                             "protocol_version": opt_text,
                             "status_code": lambda v: v is None or (is_int(v) and 0 <= v <= 999),
                             "bytes_transferred": lambda v: v is None or nonneg_int(v)},
}

# file -> function giving the unique key of a row
KEYS = {
    "batches.jsonl": lambda r: (r["batch_id"],),
    "daily_traffic.jsonl": lambda r: (r["batch_id"], r["log_date"], r["status_code"]),
    "top_resources.jsonl": lambda r: (r["batch_id"], r["resource_path"]),
    "hourly_errors.jsonl": lambda r: (r["batch_id"], r["log_date"], r["log_hour"]),
    "malformed_samples.jsonl": lambda r: (r["record_number"],),
    "phase_timings.jsonl": lambda r: (r["phase_name"],),
    "parsed_records.jsonl": lambda r: (r["record_number"],),
}


class _DuplicateField(ValueError):
    pass


def _no_duplicate_fields(pairs):
    obj = {}
    for k, v in pairs:
        if k in obj:
            raise _DuplicateField(k)
        obj[k] = v
    return obj


def _reject_constant(name):
    raise ValueError(f"{name} is not valid JSON")


def read_jsonl(path):
    """Return (rows, errors). Every line must be one JSON object."""
    rows, errors = [], []
    with open(path, "rb") as f:
        for n, raw in enumerate(f, 1):
            try:
                line = raw.decode("utf-8")
            except UnicodeDecodeError:
                errors.append(f"{Path(path).name}:{n}: not valid UTF-8")
                continue
            if not line.endswith("\n"):
                errors.append(f"{Path(path).name}:{n}: line does not end with LF")
            try:
                obj = json.loads(line, object_pairs_hook=_no_duplicate_fields,
                                 parse_constant=_reject_constant)
            except _DuplicateField as e:
                errors.append(f"{Path(path).name}:{n}: field {e} appears twice")
                continue
            except ValueError as e:
                errors.append(f"{Path(path).name}:{n}: invalid JSON ({e})")
                continue
            if not isinstance(obj, dict):
                errors.append(f"{Path(path).name}:{n}: not a JSON object")
                continue
            rows.append(obj)
    return rows, errors


def sort_key(value):
    """Order keys that may contain None (whole-run scope sorts last)."""
    return tuple((1, 0) if v is None else (0, v) for v in value)
