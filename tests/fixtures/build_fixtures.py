"""Write the fixture input files (exact bytes) and their expected outputs.

Run from the repository root:

    python -m tests.fixtures.build_fixtures

Inputs are defined here as byte strings, so CR, NUL, ISO-8859-1 bytes,
missing final LF and empty files are exact. Expected outputs come from the
test oracle (tests/oracle.py). tests/test_fixtures.py checks the oracle
against hand-written expectations from docs/PARSING_TEST_CASES.md, and
checks that the committed files match this script.
"""

import json
import shutil
from pathlib import Path

from tests import oracle

FIXTURES_DIR = Path(__file__).resolve().parent

TS = b"01/Jul/1995:00:00:01 -0400"
H = b"199.72.81.55 - - [" + TS + b"] "


def L(request, rest):
    return H + request + b" " + rest


# (case id, line bytes without terminator), in file order.
PARSING_CASES = [
    ("T01", L(b'"GET /history/apollo/ HTTP/1.0"', b"200 6245")),
    ("T02", L(b'"POST /cgi-bin/imagemap/countdown HTTP/1.0"', b"200 97")),
    ("T03", L(b'"GET /history/apollo/ HTTP/1.0"', b"200 -")),
    ("T04", H + b'"GET /history/apollo/ HTTP/1.0" 200'),
    ("T05", L(b'"-"', b"200 0")),
    ("T06", L(b'"GET /search?q=nasa HTTP/1.0"', b"200 100")),
    ("T07", L(b'"GET /history/apollo/"', b"200 100")),
    ("T08", b'199.72.81.55 - - [INVALID] "GET /history/apollo/ HTTP/1.0" 200 6245'),
    ("T09", b' - - [' + TS + b'] "GET /history/apollo/ HTTP/1.0" 200 6245'),
    ("T10", b'- - - [' + TS + b'] "GET /history/apollo/ HTTP/1.0" 200 6245'),
    ("T11", H + b'"GET /history/apollo/ HTTP/1.0"'),
    ("T12", L(b'"GET /history/apollo/ HTTP/1.0"', b"ABC 6245")),
    ("T13", L(b'"GET /history/apollo/ HTTP/1.0"', b"200 abc")),
    ("T14", H + b'"GET /history/apollo/ HTTP/1.0 200 6245'),
    ("T15", L(b'"GET /history/apollo/ HTTP/1.0 EXTRA"', b"200 6245")),
    ("T16", L(b'"GET /history HTTP/1.0"', b"404 100")),
    ("T17", L(b'"GET /history HTTP/1.0"', b"500 100")),
    ("T18", L(b'"GET /history HTTP/1.0"', b"399 100")),
    ("T19", L(b'"GET /history HTTP/1.0"', b"599 100")),
    ("T20", L(b'"GET /history HTTP/1.0"', b"600 100")),
    ("T21", L(b'""', b"200 0")),
    ("T22", L(b'"GET"', b"200 100")),
    ("T23", L(b'"GET /history"', b"200 100")),
    ("T24", b'199.72.81.55\t-\t-\t[' + TS + b']\t"GET /history/apollo/ HTTP/1.0"\t200\t6245'),
    ("T25", L(b'"GET\t/history/apollo/\tHTTP/1.0"', b"200 6245")),
    ("T26", L(b'"GET /history HTTP/1.0"', b"200 -1")),
    ("T27", b""),
    ("T28", b'199.72.81.55 - - [32/Jul/1995:00:00:01 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T29", b'199.72.81.55 - - [01/Jul/1995:25:00:00 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T30", L(b'"GET /history HTTP/1.0"', b"200 9223372036854775808")),
    ("T31", L(b'"GET /a"b HTTP/1.0"', b"200 100")),
    ("T32", L(b'"GET /history HTTP/1.0"', b"200 100  \t")),
    ("T33", L(b'"GET /history HTTP/1.0"', b"200 100 extra")),
    ("T34", b'199.72.81.55 - - [01/Jul/1995:23:59:59 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T35", b'199.72.81.55 - - [31/Jun/1995:12:00:00 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T36", b'199.72.81.55 - - [29/Feb/1995:12:00:00 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T37", b'199.72.81.55 - - [29/Feb/1996:12:00:00 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T38", b'199.72.81.55 - - [01/jul/1995:12:00:00 -0400] "GET /history HTTP/1.0" 200 100'),
    ("T39", b'199.72.81.55 - - [01/Jul/1995:12:00:00 -0460] "GET /history HTTP/1.0" 200 100'),
    ("T40", L(b'"GET /a\rb HTTP/1.0"', b"200 100")),
    ("T41", L(b'"GET /caf\xe9 HTTP/1.0"', b"200 100")),
    ("T42", b"\x00"),
    ("T43", b'199.72.81.55 ident1 frank [' + TS + b'] "GET /history HTTP/1.0" 200 100'),
    ("T44", L(b'"get /history HTTP/1.0"', b"200 100")),
    ("T45", b"   "),
    ("T46", L(b'"GET /history HTTP/1.0"', b"200 0100")),
    ("T47", L(b'"/index.html"', b"200 100")),
    ("T48", b'- - - [' + TS + b'] "GET /history HTTP/1.0" ABC 100'),
    ("T49", L(b'"GET /a\x00b HTTP/1.0"', b"200 100")),
    ("T50", b"\x00 garbage"),
]

QUERY_LINES = [
    b'h1 - - [01/Jul/1995:10:00:00 -0400] "GET /a HTTP/1.0" 200 100',
    b'h2 - - [01/Jul/1995:10:05:00 -0400] "GET /a HTTP/1.0" 404 -',
    b'h1 - - [01/Jul/1995:10:10:00 -0400] "GET /b HTTP/1.0" 200 50',
    b'h3 - - [01/Jul/1995:11:00:00 -0400] "GET /b HTTP/1.0" 500 10',
    b'',
    b'h1 - - [02/Jul/1995:00:00:00 -0400] "GET /a HTTP/1.0" 200 100',
    b'h2 - - [02/Jul/1995:00:30:00 -0400] "-" 404 0',
]


def _log(host, path, status=b"200", size=b"10", hour=b"10"):
    return (host + b" - - [01/Jul/1995:" + hour + b":00:00 -0400] \"GET " + path
            + b" HTTP/1.0\" " + status + b" " + size)


def top20_lines():
    """18 paths with unique counts 30..13, then 5 paths tied at 5 requests.

    Binary UTF-8 order of the tied paths: /tie/D < /tie/a < /tie/b < /tie/c < /tie/\xe9,
    so ranks 19 and 20 are /tie/D and /tie/a, and the rest are cut.
    """
    lines = []
    for i in range(18):
        path = b"/p%02d" % i
        for j in range(30 - i):
            lines.append(_log(b"host%d" % (j % 7), path, b"404" if j % 5 == 0 else b"200"))
    for path in (b"/tie/b", b"/tie/a", b"/tie/D", b"/tie/c", b"/tie/\xe9"):
        for j in range(5):
            lines.append(_log(b"host%d" % j, path, hour=b"11"))
    return lines


# name: (description, batch_size, [(file name, exact bytes), ...])
FIXTURES = {
    "parsing": (
        "One line per test case T01-T50 from docs/PARSING_TEST_CASES.md, LF terminated.",
        10,
        [("cases.log", b"".join(line + b"\n" for _, line in PARSING_CASES))],
    ),
    "query": (
        "Query fixture from docs/PARSING_TEST_CASES.md section 4.",
        3,
        [("query.log", b"".join(line + b"\n" for line in QUERY_LINES))],
    ),
    "boundaries": (
        "File boundaries: CRLF, a bare CR at the end of an unterminated last line, "
        "an unterminated last line, an empty file in the middle, and a file of one LF.",
        2,
        [
            ("part1.log", _log(b"a1", b"/crlf") + b"\r\n"
                          + _log(b"a2", b"/lf") + b"\n"
                          + _log(b"a3", b"/bare-cr") + b"\r"),
            ("part2.log", _log(b"b1", b"/after-boundary") + b"\n"
                          + b"\n"
                          + _log(b"b3", b"/crlf2") + b"\r\n"
                          + _log(b"b4", b"/unterminated")),
            ("part3.log", b""),
            ("part4.log", b"\n"),
        ],
    ),
    "empty": (
        "Two empty input files: zero records, zero batches.",
        5,
        [("empty1.log", b""), ("empty2.log", b"")],
    ),
    "top20": (
        "Top-20 cut-off with a tie across ranks 19-23 and binary UTF-8 ordering.",
        50,
        [("top20.log", b"".join(line + b"\n" for line in top20_lines()))],
    ),
}


def build(root=FIXTURES_DIR):
    root = Path(root)
    for name, (description, batch_size, files) in FIXTURES.items():
        d = root / name
        if d.exists():
            shutil.rmtree(d)
        (d / "input").mkdir(parents=True)
        for file_name, data in files:
            (d / "input" / file_name).write_bytes(data)
        meta = {"description": description, "batch_size": batch_size,
                "inputs": [f"input/{fn}" for fn, _ in files]}
        (d / "fixture.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8", newline="\n")
        outputs = oracle.run([d / "input" / fn for fn, _ in files], batch_size)
        oracle.write_outputs(outputs, d / "expected")
    return sorted(FIXTURES)


if __name__ == "__main__":
    for name in build():
        print("built", name)
