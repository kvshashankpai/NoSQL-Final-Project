"""Keep the docs, config, staging code and oracle in agreement."""

import re
from pathlib import Path

from etl_common.config import load_config
from etl_common.staging import iter_records
from tests import oracle

DOCS = Path(__file__).resolve().parent.parent / "docs"
REASONS = ["BLANK_LINE", "NUL_BYTE", "BAD_STRUCTURE", "BAD_HOST", "BAD_TIMESTAMP",
           "BAD_STATUS", "BAD_BYTES"]


def test_oracle_regexes_match_parsing_rules_doc():
    doc = (DOCS / "PARSING_RULES.md").read_text(encoding="utf-8")
    assert f"`{oracle.OUTER_PATTERN}`" in doc
    assert f"`{oracle.TIMESTAMP_PATTERN}`" in doc


def test_reason_codes_agree_everywhere():
    assert load_config()["output"]["malformed_reason_codes"] == REASONS
    doc = (DOCS / "PARSING_RULES.md").read_text(encoding="utf-8")
    assert re.findall(r"^\| \d \| `([A-Z_]+)` \|", doc, flags=re.M) == REASONS


def test_query_columns_agree_with_brief():
    q = {x["id"]: x["columns"] for x in load_config()["queries"]}
    assert q["Q1_Daily_Traffic_Summary"] == ["log_date", "status_code", "request_count", "total_bytes"]
    assert q["Q2_Top_Requested_Resources"] == ["resource_path", "request_count", "total_bytes",
                                               "distinct_host_count"]
    assert q["Q3_Hourly_Error_Analysis"] == ["log_date", "log_hour", "error_request_count",
                                             "total_request_count", "error_rate", "distinct_error_hosts"]


def test_staging_line_rules(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.write_bytes(b"x\r\ny\rz\nlast\r")  # CRLF, lone CR inside, bare CR at EOF
    b.write_bytes(b"\n\nend")
    recs = list(iter_records([a, b], 2))
    assert [(r.record_number, r.batch_id, r.source_file, r.text) for r in recs] == [
        (1, 1, "a", "x"), (2, 1, "a", "y\rz"), (3, 2, "a", "last\r"),
        (4, 2, "b", ""), (5, 3, "b", ""), (6, 3, "b", "end")]


def test_staging_decodes_iso_8859_1(tmp_path):
    f = tmp_path / "f"
    f.write_bytes(bytes(range(1, 256)).replace(b"\n", b"") + b"\n")
    (rec,) = iter_records([f], 10)
    assert rec.text.encode("iso-8859-1") == bytes(range(1, 256)).replace(b"\n", b"")
