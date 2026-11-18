"""Check the fixtures against hand-written expectations, independently of the oracle."""

import filecmp
import json
from pathlib import Path

import pytest

from tests import oracle
from tests.fixtures import build_fixtures

FIX = Path(__file__).resolve().parent / "fixtures"

# Hand-written from docs/PARSING_TEST_CASES.md: a reason code, or a dict of field values.
EXPECTED_CASES = {
    "T01": {"host": "199.72.81.55", "timestamp": "01/Jul/1995:00:00:01 -0400", "log_date": "1995-07-01",
            "log_hour": 0, "http_method": "GET", "resource_path": "/history/apollo/",
            "protocol_version": "HTTP/1.0", "status_code": 200, "bytes_transferred": 6245},
    "T02": {"http_method": "POST", "resource_path": "/cgi-bin/imagemap/countdown", "bytes_transferred": 97},
    "T03": {"bytes_transferred": 0}, "T04": {"bytes_transferred": 0},
    "T05": {"http_method": None, "resource_path": None, "protocol_version": None},
    "T06": {"resource_path": "/search?q=nasa"},
    "T07": {"http_method": "GET", "resource_path": "/history/apollo/", "protocol_version": None},
    "T08": "BAD_TIMESTAMP", "T09": "BAD_STRUCTURE", "T10": "BAD_HOST", "T11": "BAD_STRUCTURE",
    "T12": "BAD_STATUS", "T13": "BAD_BYTES", "T14": "BAD_STRUCTURE",
    "T15": {"http_method": "GET", "resource_path": None, "protocol_version": None},
    "T16": {"status_code": 404}, "T17": {"status_code": 500}, "T18": {"status_code": 399},
    "T19": {"status_code": 599}, "T20": {"status_code": 600},
    "T21": {"http_method": None, "resource_path": None, "protocol_version": None},
    "T22": {"http_method": "GET", "resource_path": None, "protocol_version": None},
    "T23": {"http_method": "GET", "resource_path": "/history", "protocol_version": None},
    "T24": {"http_method": "GET", "resource_path": "/history/apollo/", "protocol_version": "HTTP/1.0",
            "bytes_transferred": 6245},
    "T25": {"http_method": "GET", "resource_path": "/history/apollo/", "protocol_version": "HTTP/1.0"},
    "T26": "BAD_BYTES", "T27": "BLANK_LINE", "T28": "BAD_TIMESTAMP", "T29": "BAD_TIMESTAMP",
    "T30": "BAD_BYTES",
    "T31": {"http_method": "GET", "resource_path": '/a"b', "protocol_version": "HTTP/1.0"},
    "T32": {"bytes_transferred": 100}, "T33": "BAD_STRUCTURE",
    "T34": {"log_date": "1995-07-01", "log_hour": 23},
    "T35": "BAD_TIMESTAMP", "T36": "BAD_TIMESTAMP",
    "T37": {"log_date": "1996-02-29", "log_hour": 12},
    "T38": "BAD_TIMESTAMP", "T39": "BAD_TIMESTAMP",
    "T40": {"resource_path": "/a\rb", "protocol_version": "HTTP/1.0"},
    "T41": {"resource_path": "/café"},
    "T42": "NUL_BYTE",
    "T43": {"host": "199.72.81.55", "resource_path": "/history"},
    "T44": {"http_method": "get"}, "T45": "BAD_STRUCTURE", "T46": {"bytes_transferred": 100},
    "T47": {"http_method": "/index.html", "resource_path": None, "protocol_version": None},
    "T48": "BAD_HOST", "T49": "NUL_BYTE", "T50": "NUL_BYTE",
}


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def test_every_documented_case_is_in_the_fixture():
    assert [cid for cid, _ in build_fixtures.PARSING_CASES] == list(EXPECTED_CASES)


def test_parsing_fixture_matches_documented_expectations():
    parsed = read_jsonl(FIX / "parsing" / "expected" / "parsed_records.jsonl")
    assert len(parsed) == len(EXPECTED_CASES)
    for row, (cid, expected) in zip(parsed, EXPECTED_CASES.items()):
        if isinstance(expected, str):
            assert (row["valid"], row["reason"]) == (False, expected), cid
            assert all(row[f] is None for f in oracle.FIELDS), cid
        else:
            assert row["valid"] and row["reason"] is None, cid
            for field, value in expected.items():
                assert row[field] == value, (cid, field)


def test_query_fixture_matches_doc_tables():
    exp = FIX / "query" / "expected"
    assert read_jsonl(exp / "batches.jsonl") == [
        {"batch_id": 1, "record_count": 3, "malformed_count": 0},
        {"batch_id": 2, "record_count": 3, "malformed_count": 1},
        {"batch_id": 3, "record_count": 1, "malformed_count": 0}]
    q1 = {(r["batch_id"], r["log_date"], r["status_code"]): (r["request_count"], r["total_bytes"])
          for r in read_jsonl(exp / "daily_traffic.jsonl")}
    assert q1 == {(1, "1995-07-01", 200): (2, 150), (1, "1995-07-01", 404): (1, 0),
                  (2, "1995-07-01", 500): (1, 10), (2, "1995-07-02", 200): (1, 100),
                  (3, "1995-07-02", 404): (1, 0),
                  (None, "1995-07-01", 200): (2, 150), (None, "1995-07-01", 404): (1, 0),
                  (None, "1995-07-01", 500): (1, 10), (None, "1995-07-02", 200): (1, 100),
                  (None, "1995-07-02", 404): (1, 0)}
    q2 = [(r["batch_id"], r["rank"], r["resource_path"], r["request_count"], r["total_bytes"],
           r["distinct_host_count"]) for r in read_jsonl(exp / "top_resources.jsonl")]
    assert q2 == [(1, 1, "/a", 2, 100, 2), (1, 2, "/b", 1, 50, 1), (2, 1, "/a", 1, 100, 1),
                  (2, 2, "/b", 1, 10, 1), (None, 1, "/a", 3, 200, 2), (None, 2, "/b", 2, 60, 2)]
    q3 = {(r["batch_id"], r["log_date"], r["log_hour"]):
          (r["error_request_count"], r["total_request_count"], r["distinct_error_hosts"])
          for r in read_jsonl(exp / "hourly_errors.jsonl")}
    assert q3 == {(1, "1995-07-01", 10): (1, 3, 1), (2, "1995-07-01", 11): (1, 1, 1),
                  (2, "1995-07-02", 0): (0, 1, 0), (3, "1995-07-02", 0): (1, 1, 1),
                  (None, "1995-07-01", 10): (1, 3, 1), (None, "1995-07-01", 11): (1, 1, 1),
                  (None, "1995-07-02", 0): (1, 2, 1)}
    samples = read_jsonl(exp / "malformed_samples.jsonl")
    assert samples == [{"source_file": "query.log", "record_number": 5, "batch_id": 2,
                        "reason": "BLANK_LINE", "raw_line": ""}]


def test_boundaries_fixture():
    parsed = read_jsonl(FIX / "boundaries" / "expected" / "parsed_records.jsonl")
    got = [(r["record_number"], r["batch_id"], r["source_file"], r["reason"], r["resource_path"])
           for r in parsed]
    assert got == [
        (1, 1, "part1.log", None, "/crlf"),            # CRLF terminator removed
        (2, 1, "part1.log", None, "/lf"),
        (3, 2, "part1.log", "BAD_BYTES", None),        # bare CR at EOF kept as content
        (4, 2, "part2.log", None, "/after-boundary"),  # not joined to record 3
        (5, 3, "part2.log", "BLANK_LINE", None),
        (6, 3, "part2.log", None, "/crlf2"),
        (7, 4, "part2.log", None, "/unterminated"),    # unterminated last line counted
        (8, 4, "part4.log", "BLANK_LINE", None),       # empty part3.log adds nothing
    ]


def test_empty_fixture_has_no_rows():
    exp = FIX / "empty" / "expected"
    assert all(p.stat().st_size == 0 for p in exp.iterdir())


def test_top20_cutoff_and_binary_order():
    rows = [r for r in read_jsonl(FIX / "top20" / "expected" / "top_resources.jsonl")
            if r["batch_id"] is None]
    assert [r["rank"] for r in rows] == list(range(1, 21))
    assert [r["resource_path"] for r in rows[18:]] == ["/tie/D", "/tie/a"]


def test_committed_fixtures_match_build_script(tmp_path):
    build_fixtures.build(tmp_path)
    for name in build_fixtures.FIXTURES:
        cmp = filecmp.dircmp(FIX / name, tmp_path / name)
        _assert_same(cmp, name)


def _assert_same(cmp, where):
    assert not cmp.left_only and not cmp.right_only, (where, cmp.left_only, cmp.right_only)
    _, mismatch, errors = filecmp.cmpfiles(cmp.left, cmp.right, cmp.common_files, shallow=False)
    assert not mismatch and not errors, (where, mismatch, errors)
    for sub, subcmp in cmp.subdirs.items():
        _assert_same(subcmp, f"{where}/{sub}")


@pytest.mark.parametrize("name", sorted(build_fixtures.FIXTURES))
def test_expected_outputs_pass_the_validator(name):
    from controller.validation import validate_output
    meta = json.loads((FIX / name / "fixture.json").read_text(encoding="utf-8"))
    result = validate_output(FIX / name / "expected", meta["batch_size"], require_parsed=True)
    assert result.errors == []
