# Environment for WSL (single-node Hadoop, Pig, user-space PostgreSQL). Adjust paths to your machine.
export NOSQL_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # repository root
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export PIG_HOME=$HOME/pig-0.18.0
# PostgreSQL 16 unpacked under ~/pgroot (no root needed), data in ~/pgdata
export PG_BIN=$HOME/pgroot/usr/lib/postgresql/16/bin
export LD_LIBRARY_PATH=$HOME/pgroot/usr/lib/x86_64-linux-gnu
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$PG_BIN:$PATH
export PYTHONPATH=$HOME/etlpy          # psycopg, pytest
export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGDATABASE=nosql_final_project
# The NASA logs, copied to the Linux file system for fast reads
export NASA_DIR=$HOME/nasa
export NASA_INPUTS="--input $NASA_DIR/NASA_access_log_Jul95 --input $NASA_DIR/NASA_access_log_Aug95"
