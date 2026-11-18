"""MongoDB adapter against the shared fixtures. Skipped when MongoDB is not reachable."""

from pathlib import Path

import pytest

from controller.fixture_runner import fixture_names, run_fixture
from pipelines.mongodb import pipeline as P
from tests import oracle

DOCS = Path(__file__).resolve().parent.parent / "docs"


def test_pipeline_patterns_match_parsing_rules():
    doc = (DOCS / "PARSING_RULES.md").read_text(encoding="utf-8")
    assert f"`{P.OUTER_PATTERN}`" in doc and f"`{P.TIMESTAMP_PATTERN}`" in doc
    assert (P.OUTER_PATTERN, P.TIMESTAMP_PATTERN) == (oracle.OUTER_PATTERN, oracle.TIMESTAMP_PATTERN)


def _mongo_available():
    try:
        from pipelines.mongodb.adapter import connect
        connect().close()
        return True
    except Exception:
        return False


needs_mongo = pytest.mark.skipif(not _mongo_available(), reason="MongoDB not reachable (MONGODB_URI)")


@needs_mongo
@pytest.mark.parametrize("name", fixture_names())
def test_mongodb_fixture(name, tmp_path):
    ok, detail = run_fixture("MongoDB", name, tmp_path)
    assert ok, detail
