# Environment and Launch Instructions

## DAS 839 - NoSQL Systems End-Semester Project

## 1. Where to run

Run the whole tool (controller, adapters, engines and databases) on one Linux machine. On Windows, use **WSL2 with Ubuntu 24.04**:

- Hadoop, Pig and Hive do not run natively on Windows without extra native binaries.
- Clone the repository inside the WSL file system (for example `~/NoSQL-Final-Project`), not under `/mnt/c/...` or a OneDrive folder. Cross-file-system and sync I/O are slow, and they would distort the runtime measurements.

Use the same machine and the same engine execution mode for every benchmark run (`EXPERIMENT_DESIGN.md`).

## 2. Versions

| Component | Version | Notes |
|---|---|---|
| Python | 3.11 or later | Controller, adapters' launch scripts, tools |
| PostgreSQL | 16 (15 minimum) | `UNIQUE NULLS NOT DISTINCT` needs 15+ |
| Java for Hadoop and Pig | OpenJDK 8 | Supported by Hadoop 3.3 and Pig 0.18 |
| Hadoop | 3.3.6 | Single-node pseudo-distributed (HDFS + YARN); also runs the MapReduce adapter |
| Pig | 0.18.0 | Apache documents Hadoop 3 support for this release; validate the chosen runtime with fixtures during implementation |

The Pig and Hadoop versions are the targets for the adapters that are not written yet. Record the versions actually used in `runs.engine_info` (each adapter reports them with `--engine-info`).

Python packages are listed in `requirements.txt`:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## 3. Connection settings

Copy `.env.example` to `.env` (git-ignored) and edit it. The adapters and tools read it; real environment variables take precedence.

| Variable | Used by | Default |
|---|---|---|
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` | PostgreSQL loading and reporting | libpq defaults |
| `PGDATABASE` | PostgreSQL loading and reporting | `nosql_final_project` (from the config) |

## 4. Installing and starting services (Ubuntu 24.04)

### PostgreSQL 16

```bash
sudo apt install postgresql
sudo service postgresql start
sudo -u postgres createuser --pwprompt etl
sudo -u postgres createdb -O etl nosql_final_project
sudo -u postgres createdb -O etl nosql_test
psql -v ON_ERROR_STOP=1 -d nosql_final_project -f database/schema.sql
```

### Hadoop and Pig

Install OpenJDK 8 and unpack the release archives under `/opt`. Set `JAVA_HOME`, `HADOOP_HOME` and `PIG_HOME`, and add their `bin` directories to `PATH`. Configure Hadoop as a single-node pseudo-distributed cluster (Hadoop documentation: "Single Node Setup"), then:

```bash
hdfs namenode -format              # first time only
start-dfs.sh && start-yarn.sh
```

The Pig and MapReduce adapters must run in this same mode, and they must set `textinputformat.record.delimiter` to `\n` (`PARSING_RULES.md` section 11).

### Windows portable setup (development checks only)

PostgreSQL can also run on Windows from its official zip archive, without installation:

```bash
# PostgreSQL 16.15 (postgresql-16.15-1-windows-x64-binaries.zip), unpacked to C:\Users\<you>\etl-services
pgsql/bin/initdb.exe -D pgdata -U postgres -A trust -E UTF8 --locale=C
pgsql/bin/pg_ctl.exe -D pgdata -l postgres.log -o "-p 55432 -c listen_addresses=localhost" -w start
pgsql/bin/createdb.exe -h localhost -p 55432 -U postgres nosql_final_project
```

with `.env`: `PGHOST=localhost`, `PGPORT=55432`, `PGUSER=postgres`. `-A trust` has no password, so use it on a single-user machine only. Do not use this setup for benchmarks; use the Linux setup above.

## 5. Running

```bash
python scripts/verify_inputs.py                                  # once per benchmark session (not timed)
python -m controller.fixture_runner --pipeline Pig               # adapter vs. expected fixture outputs
python -m controller.validation OUTPUT_DIR --batch-size N        # check one output directory
python -m controller.compare EXPECTED_DIR ACTUAL_DIR             # row-by-row comparison, both directions
python -m pytest                                                 # offline tests
```

The controller's `run` command, the PostgreSQL loader and the report come with the PostgreSQL layer.

## 6. Adapter launch commands

The controller runs `launcher + --batch-size N --output-dir DIR --input FILE ...` from the repository root. `{python}` is replaced by the controller's own interpreter. Fill in each launcher in `config/project_config.json` when the adapter exists; the controller refuses to run an adapter whose launcher is `null`.

| Pipeline | Launcher | Status |
|---|---|---|
| Pig | `null` | In progress |
| MapReduce | `{python} pipelines/mapreduce/adapter.py` | Phase 1 prototype |

The team is running only the Pig and MapReduce pipelines. The MongoDB and Hive entries in the config have no launcher.

Every adapter must also support `--debug-parsed` (fixtures) and `--engine-info` (prints a JSON object with engine name, version and mode).

## 7. Optional database service configuration

compose.yaml provides a PostgreSQL 16 development service.
Set PGPASSWORD in .env (or the shell) and keep it private. The PostgreSQL
service creates user etl and database nosql_final_project to match .env.example.
Its host port is 5433 (container port 5432), bound to localhost, to avoid a
conflict with a local PostgreSQL installation.

~~~bash
docker compose up -d postgres
psql -h localhost -p 5433 -U etl -v ON_ERROR_STOP=1 -d nosql_final_project -f database/schema.sql
~~~

The Compose file only starts shared databases. It does not run adapters,
the application controller, or benchmarks. Existing native services can
be used instead. Choose one service setup to avoid port conflicts.

The schema accepts every malformed reason in the config, including NUL_BYTE.
Reapply it after updating to install the NUL_BYTE constraint migration.
Generated verification/ files are local artifacts and are excluded from Git.

Pig 0.18 Hadoop compatibility is documented by Apache:
https://pig.apache.org/releases.html

This update covers shared setup and database contracts. Controller/loader,
recovery implementation, frontend and additional pipeline work remain for
the implementation phase.
