"""TEST ORACLE ONLY. Not a pipeline.

A small, direct Python reading of docs/PARSING_RULES.md and the query
definitions. It generates the expected output files for the fixtures in
tests/fixtures/. It is never run by the controller, never selectable as a
pipeline, and never used on the NASA dataset for reported results.
"""

import calendar
import json
import re
from pathlib import Path

from etl_common.staging import iter_records

# Copied from docs/PARSING_RULES.md. tests/test_contract.py checks they still match.
OUTER_PATTERN = (
    r'^([^ \t]+)[ \t]+([^ \t]+)[ \t]+([^ \t]+)[ \t]+\[([^\]]*)\][ \t]+"(.*)"'
    r'[ \t]+([^ \t]+)(?:[ \t]+([^ \t]+))?[ \t]*$'
)
TIMESTAMP_PATTERN = (
    r'^(\d{2})/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/(\d{4}):'
    r'(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$'
)
OUTER = re.compile(OUTER_PATTERN)
TIMESTAMP = re.compile(TIMESTAMP_PATTERN)
MONTHS = {abbr: i for i, abbr in enumerate(calendar.month_abbr) if abbr}
MAX_BIGINT = 2**63 - 1
TOP_N = 20
SAMPLE_LIMIT = 100

FIELDS = ("host", "timestamp", "log_date", "log_hour", "http_method",
          "resource_path", "protocol_version", "status_code", "bytes_transferred")


def parse(text):
    """Return (reason, None) for a malformed record or (None, fields) for a valid one."""
    if text == "":
        return "BLANK_LINE", None
    if "\x00" in text:
        return "NUL_BYTE", None
    m = OUTER.match(text)
    if not m:
        return "BAD_STRUCTURE", None
    host, _ident, _user, ts, request, status, nbytes = m.groups()
    if host == "-":
        return "BAD_HOST", None
    t = TIMESTAMP.match(ts)
    if not t:
        return "BAD_TIMESTAMP", None
    day, mon, year, hh, mi, ss, _sign, oh, om = t.groups()
    day, year, hh, mi, ss, oh, om = map(int, (day, year, hh, mi, ss, oh, om))
    month = MONTHS[mon]
    if year < 1 or not (1 <= day <= calendar.monthrange(year, month)[1]):
        return "BAD_TIMESTAMP", None
    if not (hh <= 23 and mi <= 59 and ss <= 59 and oh <= 23 and om <= 59):
        return "BAD_TIMESTAMP", None
    if not re.fullmatch(r"[0-9]{3}", status):
        return "BAD_STATUS", None
    if nbytes is None or nbytes == "-":
        size = 0
    elif re.fullmatch(r"[0-9]+", nbytes) and int(nbytes) <= MAX_BIGINT:
        size = int(nbytes)
    else:
        return "BAD_BYTES", None
    stripped = request.strip(" \t")
    tokens = [] if stripped in ("", "-") else re.split(r"[ \t]+", stripped)
    return None, {
        "host": host,
        "timestamp": ts,
        "log_date": f"{year:04d}-{month:02d}-{day:02d}",
        "log_hour": hh,
        "http_method": tokens[0] if tokens else None,
        "resource_path": tokens[1] if 2 <= len(tokens) <= 3 else None,
        "protocol_version": tokens[2] if len(tokens) == 3 else None,
        "status_code": int(status),
        "bytes_transferred": size,
    }


def _queries(valid, batch_id):
    q1, q2, q3 = {}, {}, {}
    for f in valid:
        a = q1.setdefault((f["log_date"], f["status_code"]), [0, 0])
        a[0] += 1
        a[1] += f["bytes_transferred"]
        if f["resource_path"] is not None:
            a = q2.setdefault(f["resource_path"], [0, 0, set()])
            a[0] += 1
            a[1] += f["bytes_transferred"]
            a[2].add(f["host"])
        a = q3.setdefault((f["log_date"], f["log_hour"]), [0, 0, set()])
        a[1] += 1
        if 400 <= f["status_code"] <= 599:
            a[0] += 1
            a[2].add(f["host"])
    daily = [{"batch_id": batch_id, "log_date": d, "status_code": s,
              "request_count": c, "total_bytes": b}
             for (d, s), (c, b) in sorted(q1.items())]
    ranked = sorted(q2.items(), key=lambda kv: (-kv[1][0], kv[0].encode("utf-8")))[:TOP_N]
    top = [{"batch_id": batch_id, "rank": i, "resource_path": p, "request_count": c,
            "total_bytes": b, "distinct_host_count": len(h)}
           for i, (p, (c, b, h)) in enumerate(ranked, 1)]
    hourly = [{"batch_id": batch_id, "log_date": d, "log_hour": h,
               "error_request_count": e, "total_request_count": t,
               "error_rate": e / t, "distinct_error_hosts": len(hs)}
              for (d, h), (e, t, hs) in sorted(q3.items())]
    return daily, top, hourly


def run(input_files, batch_size):
    """Return {file_name: [row, ...]} for every output file, plus parsed_records."""
    parsed, batches, samples = [], {}, []
    for rec in iter_records(input_files, batch_size):
        reason, fields = parse(rec.text)
        row = {"record_number": rec.record_number, "batch_id": rec.batch_id,
               "source_file": rec.source_file, "valid": reason is None, "reason": reason}
        row.update(fields if fields else {k: None for k in FIELDS})
        parsed.append(row)
        b = batches.setdefault(rec.batch_id, [0, 0])
        b[0] += 1
        if reason:
            b[1] += 1
            if len(samples) < SAMPLE_LIMIT:
                samples.append({"source_file": rec.source_file, "record_number": rec.record_number,
                                "batch_id": rec.batch_id, "reason": reason, "raw_line": rec.text})
    out = {"batches.jsonl": [{"batch_id": k, "record_count": c, "malformed_count": m}
                             for k, (c, m) in sorted(batches.items())],
           "daily_traffic.jsonl": [], "top_resources.jsonl": [], "hourly_errors.jsonl": [],
           "malformed_samples.jsonl": samples, "parsed_records.jsonl": parsed}
    scopes = [(k, [r for r in parsed if r["batch_id"] == k and r["valid"]]) for k in sorted(batches)]
    scopes.append((None, [r for r in parsed if r["valid"]]))
    for batch_id, valid in scopes:
        daily, top, hourly = _queries(valid, batch_id)
        out["daily_traffic.jsonl"] += daily
        out["top_resources.jsonl"] += top
        out["hourly_errors.jsonl"] += hourly
    return out


def write_outputs(outputs, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in outputs.items():
        with open(out_dir / name, "w", encoding="utf-8", newline="\n") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
