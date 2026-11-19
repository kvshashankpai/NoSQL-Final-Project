# Start HDFS, YARN, the JobHistory server and PostgreSQL. Safe to run again.
source "$(dirname "$0")/env.sh"
jps | grep -q NameNode         || setsid start-dfs.sh  >/dev/null 2>&1 < /dev/null
jps | grep -q ResourceManager  || setsid start-yarn.sh >/dev/null 2>&1 < /dev/null
jps | grep -q JobHistoryServer || setsid mapred --daemon start historyserver < /dev/null
hdfs dfsadmin -safemode wait >/dev/null 2>&1
pg_ctl -D ~/pgdata status >/dev/null 2>&1 || \
  pg_ctl -D ~/pgdata -l ~/pg.log -o "-p 5432 -k $HOME/pgsock -c listen_addresses=localhost -c jit=off" -w start >/dev/null
PGOPTIONS="-c client_min_messages=warning" psql -q -v ON_ERROR_STOP=1 -f "$NOSQL_HOME/database/schema.sql" >/dev/null && echo "PostgreSQL ready (schema applied)"
jps | grep -v Jps | awk '{print "running:", $2}'
