# Usage: bash fixtures.sh <Pig|MapReduce> [local|yarn]
# Runs the adapter on the five test fixtures and compares with the expected outputs.
# Use local: fixtures have tiny batches, and every batch is several YARN jobs.
source "$(dirname "$0")/env.sh"
export HADOOP_EXECUTION_MODE=${2:-local}
cd "$NOSQL_HOME"
python3 -m controller.fixture_runner --pipeline "$1"
