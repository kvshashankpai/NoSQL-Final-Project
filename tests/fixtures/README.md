# Test fixtures

Executable versions of `docs/PARSING_TEST_CASES.md`. Every adapter must produce exactly the expected outputs for every fixture before it is run on the NASA dataset.

| Fixture | Batch size | What it tests |
|---|---:|---|
| `parsing` | 10 | One line per test case T01-T50: field extraction and every malformed reason code, including real CR, NUL and ISO-8859-1 (`0xE9`) bytes |
| `query` | 3 | The query example from section 4 of the test-case doc: per-batch and whole-run Q1-Q3, distinct hosts across batches, a Q2 tie, a NULL path, an hour with no errors |
| `boundaries` | 2 | Four input files: CRLF, a bare CR at the end of an unterminated last line (kept as content), an unterminated last line, an empty file in the middle, a file of one LF |
| `empty` | 5 | Two empty files: no records, no batches, every output file empty |
| `top20` | 50 | The top-20 cut-off with a tie across ranks 19-23, sorted in binary UTF-8 order |

Each fixture directory has:

- `fixture.json`: batch size and the input files, in processing order.
- `input/`: the exact input bytes.
- `expected/`: the expected output files, including `parsed_records.jsonl` (written by adapters run with `--debug-parsed`).

## Checking an adapter

Set the adapter's `launcher` in `config/project_config.json`, then run:

```bash
python -m controller.fixture_runner --pipeline Pig
python -m controller.fixture_runner --pipeline MapReduce --fixture parsing
```

For each fixture this runs the adapter into a temporary directory, validates the output (`controller/validation.py`), and compares every file with `expected/` in both directions (`controller/compare.py`). A failure lists missing, extra and changed rows.

An adapter must:

- accept `--batch-size N --output-dir DIR --input FILE [--input FILE ...]`, plus `--debug-parsed` and `--engine-info`;
- number records and assign batches with `etl_common/staging.py` (decision D1 in `docs/BATCHING_STRATEGY.md`);
- write the files described in `docs/COMMON_DATA_MODEL.md`.

## Changing the fixtures

Never edit files under `input/` or `expected/` by hand. Change `build_fixtures.py`, then regenerate:

```bash
python -m tests.fixtures.build_fixtures
python -m pytest tests/test_fixtures.py
```

The expected outputs come from `tests/oracle.py`, a test-only reference that is never a pipeline. `tests/test_fixtures.py` checks them against expectations written by hand from the docs. `.gitattributes` keeps Git from changing line endings in these files.
