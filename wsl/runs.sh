# Usage: bash runs.sh            list every stored run
#        bash runs.sh RUN_ID     summary of one run (add --full for every row)
source "$(dirname "$0")/env.sh"
cd "$NOSQL_HOME"
if [ -n "$1" ]; then python3 run.py --report "$@"; else python3 run.py --list; fi
