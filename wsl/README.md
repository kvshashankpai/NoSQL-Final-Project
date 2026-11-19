# Running Pig and MapReduce on WSL

Helper scripts for one Windows machine with WSL (Ubuntu): single-node Hadoop 3.3.6,
Pig 0.18.0, OpenJDK 11 and PostgreSQL 16. `env.sh` holds every path; edit it for your
machine. All commands run inside WSL from the repository root.

```bash
bash wsl/start_services.sh             # HDFS, YARN, JobHistory, PostgreSQL; applies database/schema.sql
bash wsl/run.sh MapReduce local        # one timed run, loaded into PostgreSQL
bash wsl/run.sh Pig yarn 500000        # pipeline, mode (default yarn), batch size (default 1,000,000)
bash wsl/runs.sh                       # list stored runs
bash wsl/runs.sh RUN_ID [--full]       # summary of one run, or every result row
bash wsl/fixtures.sh Pig               # the five test fixtures (local mode by default)
bash wsl/build.sh                      # rebuild both JARs after Java changes
bash wsl/stop_services.sh
```

`run.sh` calls `python3 run.py` (repository root), which works for both pipelines:
input verification (not timed), adapter run, output validation, one PostgreSQL
transaction, then the runtime is recorded. Run output goes to `~/nosql-runs/<run_id>/`
(Linux file system; `/mnt/c` and OneDrive I/O would distort timings).

## Execution mode

`HADOOP_EXECUTION_MODE` (the second `run.sh` argument) applies to both pipelines; see
`docs/ENVIRONMENT.md`. Compare runtimes only within one mode. In `yarn` mode every Hadoop job
pays YARN startup (about 10-30 s here); Pig runs about 9 jobs per batch and MapReduce 4.
The fixtures use batch sizes of 2-5, so run them in `local` mode.

Measured on one laptop (full dataset, batch size 1,000,000, including the PostgreSQL load):

| Pipeline | local | yarn |
|---|---|---|
| Pig | 154 s | 891 s |
| MapReduce | 179 s | 401 s |

## One-time setup without root

- PostgreSQL: `apt-get download postgresql-16 postgresql-client-16 libpq5`, then
  `dpkg -x <each .deb> ~/pgroot` and `initdb -D ~/pgdata -U postgres -A trust -E UTF8 --locale=C`
  (trust authentication; localhost only, single-user machine).
- Python packages: `python3 -m pip install --target ~/etlpy "psycopg[binary]" pytest`.
- Copy the NASA logs to `~/nasa` (Linux file system).
- Hadoop on WSL: start daemons with `setsid` (the scripts do) so they outlive the shell, and
  keep the JobHistory server running; Pig waits for it after every job.
