"""Record numbering and batch assignment shared by all four adapters (decision D1).

This is the only code that reads the original input files. It applies the
input rules from docs/BATCHING_STRATEGY.md section 3 and nothing else:

- Only LF ends a record. A CR directly before LF is part of the terminator.
  A CR anywhere else, including at the end of a file's unterminated last
  line, is record content.
- A file's unterminated last line is its own record.
- A trailing LF does not create an extra record.
- Bytes are decoded as ISO-8859-1, so every byte maps to one character.

It does not parse, clean, filter or convert any field. Each adapter runs it
as its first stage, inside the measured runtime.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

ENCODING = "iso-8859-1"
STAGED_FILE_NAME = "records.txt"


@dataclass(frozen=True)
class RawRecord:
    record_number: int
    batch_id: int
    source_file: str
    text: str


def batch_id_for(record_number: int, batch_size: int) -> int:
    return (record_number - 1) // batch_size + 1


def iter_file_lines(path) -> Iterator[bytes]:
    """Yield each record of one file as bytes, with its terminator removed.

    Iterating a file opened in binary mode splits on LF only, so a lone CR
    never ends a record.
    """
    with open(path, "rb") as f:
        for line in f:
            if line.endswith(b"\n"):
                line = line[:-1]
                if line.endswith(b"\r"):
                    line = line[:-1]
            yield line


def iter_raw_lines(input_files: Iterable) -> Iterator[tuple]:
    """Yield (record_number, source_file_name, raw_bytes) across all files in order."""
    record_number = 0
    for path in input_files:
        name = Path(path).name
        for line in iter_file_lines(path):
            record_number += 1
            yield record_number, name, line


def iter_records(input_files: Iterable, batch_size: int) -> Iterator[RawRecord]:
    """Yield every input record with its continuous number and batch ID."""
    if batch_size <= 0:
        raise ValueError("Batch size must be a positive integer.")
    for record_number, name, line in iter_raw_lines(input_files):
        yield RawRecord(
            record_number,
            batch_id_for(record_number, batch_size),
            name,
            line.decode(ENCODING),
        )


def iter_batches(input_files: Iterable, batch_size: int) -> Iterator[tuple]:
    """Yield (batch_id, [RawRecord, ...]) one non-empty batch at a time."""
    current, records = None, []
    for record in iter_records(input_files, batch_size):
        if record.batch_id != current and records:
            yield current, records
            records = []
        current = record.batch_id
        records.append(record)
    if records:
        yield current, records


def staged_line(record: RawRecord) -> str:
    """Staged text format for the Hadoop-based engines.

    `record_number TAB source_file TAB text`, followed by LF. The text is the
    unchanged record content and may itself contain tabs or CR characters, so
    readers split on the first two tabs only and end records at LF only.
    """
    return f"{record.record_number}\t{record.source_file}\t{record.text}\n"


def write_staged_batches(input_files: Iterable, batch_size: int, staging_dir) -> list:
    """Write staging_dir/batch_id=N/records.txt (UTF-8) for each non-empty batch.

    Returns [(batch_id, record_count), ...]. Used by the Pig, Hive and
    MapReduce adapters.
    """
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=False)
    written = []
    for batch_id, records in iter_batches(input_files, batch_size):
        batch_dir = staging_dir / f"batch_id={batch_id}"
        batch_dir.mkdir()
        with open(batch_dir / STAGED_FILE_NAME, "w", encoding="utf-8", newline="\n") as out:
            for record in records:
                out.write(staged_line(record))
        written.append((batch_id, len(records)))
    return written
