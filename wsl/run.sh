# Usage: bash run.sh <Pig|MapReduce> [local|yarn] [batch_size]
# One timed run on the full NASA logs, loaded into PostgreSQL; prints a summary.
source "$(dirname "$0")/env.sh"
[ -n "$1" ] || { echo "usage: bash run.sh <Pig|MapReduce> [local|yarn] [batch_size]"; exit 2; }
export HADOOP_EXECUTION_MODE=${2:-yarn}
cd "$NOSQL_HOME"
python3 run.py --pipeline "$1" --batch-size "${3:-1000000}" $NASA_INPUTS
