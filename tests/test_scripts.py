"""Diagnostic splitter and input verification."""

import json
import os
from pathlib import Path

import pytest

from scripts.create_batches import INCOMPLETE_MARKER, create_batches
from scripts.verify_inputs import build_record, check_current, write_record

FIX = Path(__file__).resolve().parent / "fixtures"


def test_splitter_empty_input_has_null_average(tmp_path):
    inputs = [FIX / "empty" / "input" / "empty1.log", FIX / "empty" / "input" / "empty2.log"]
    manifest, run_dir = create_batches(inputs, 5, "E", tmp_path)
    assert manifest["status"] == "complete"
    assert manifest["total_batches"] == 0 and manifest["average_batch_size"] is None
    saved = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert saved["average_batch_size"] is None
    assert not (run_dir / INCOMPLETE_MARKER).exists()


def test_splitter_preserves_bare_cr_at_file_boundary(tmp_path):
    d = FIX / "boundaries" / "input"
    inputs = [d / "part1.log", d / "part2.log", d / "part3.log", d / "part4.log"]
    manifest, run_dir = create_batches(inputs, 2, "B", tmp_path)
    assert [b["record_count"] for b in manifest["batches"]] == [2, 2, 2, 2]
    batch2 = (run_dir / "batch_0002.log").read_bytes().split(b"\n")
    assert batch2[0].endswith(b"\r")                     # record 3: bare CR kept as content
    assert batch2[1].startswith(b"b1 ")                  # record 4 not joined to it
    batch1 = (run_dir / "batch_0001.log").read_bytes()
    assert b"\r" not in batch1                           # CRLF terminator normalised
    assert manifest["average_batch_size"] == 2.0


def test_splitter_marks_incomplete_output_on_failure(tmp_path, monkeypatch):
    import scripts.create_batches as cb

    def boom(_files):
        yield 1, "x.log", b"line"
        raise OSError("disk full")
    monkeypatch.setattr(cb, "iter_raw_lines", boom)
    src = tmp_path / "x.log"
    src.write_bytes(b"line\n")
    with pytest.raises(OSError):
        create_batches([src], 5, "F", tmp_path / "out")
    run_dir = tmp_path / "out" / "F"
    assert (run_dir / INCOMPLETE_MARKER).exists()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed" and "disk full" in manifest["error"]


def test_splitter_reads_defaults_from_config():
    import scripts.create_batches as cb
    from etl_common.config import load_config
    parser_defaults = load_config()["batching"]["default_batch_size"]
    assert cb.default_batch_size() == parser_defaults


def test_verification_record_and_stat_check(tmp_path):
    a, b = tmp_path / "a.log", tmp_path / "b.log"
    a.write_bytes(b"abc\n")
    b.write_bytes(b"")
    rec = build_record([a, b], "ISO-8859-1")
    assert [f["name"] for f in rec["files"]] == ["a.log", "b.log"]
    assert rec["files"][0]["sha256"] == "edeaaff3f1774ad2888673770c6d64097e391bc362d7d6fb34982ddf0efd18cb"
    assert rec["files"][1]["size_bytes"] == 0
    path = write_record(rec, tmp_path / "v")
    assert (tmp_path / "v" / "latest.json").read_text() == path.read_text()
    assert check_current(rec, [a, b]) == []
    assert check_current(rec, [b, a])                     # wrong order
    a.write_bytes(b"abcd\n")
    os.utime(a, ns=(1, 1))
    assert any("changed since verification" in p for p in check_current(rec, [a, b]))


def test_verification_rejects_changed_file_during_hash(tmp_path, monkeypatch):
    import scripts.verify_inputs as verification
    source = tmp_path / "source.log"
    source.write_bytes(b"before")
    original = verification.sha256_of

    def hash_and_change(path):
        digest = original(path)
        Path(path).write_bytes(b"after hashing with different size")
        return digest

    monkeypatch.setattr(verification, "sha256_of", hash_and_change)
    with pytest.raises(ValueError, match="changed while hashing"):
        verification.build_record([source], "ISO-8859-1")


def test_verification_checks_paths_not_only_basename(tmp_path):
    first, second = tmp_path / "a" / "same.log", tmp_path / "b" / "same.log"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_bytes(b"same")
    second.write_bytes(b"same")
    os.utime(first, ns=(1, 1))
    os.utime(second, ns=(1, 1))
    record = build_record([first], "ISO-8859-1")
    assert any("path mismatch" in error for error in check_current(record, [second]))
