source "$(dirname "$0")/env.sh"
mapred --daemon stop historyserver; stop-yarn.sh; stop-dfs.sh
pg_ctl -D ~/pgdata stop
