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
| MongoDB | 7.0 or 8.0 (5.0 minimum) | `$setWindowFields` and `$getField` need 5.0+ |
| Java for Hadoop/Hive/Pig | OpenJDK 8 | Hive 3.1 supports Java 8 only |
| Hadoop | 3.3.6 | Single-node pseudo-distributed (HDFS + YARN) |
| Hive | 3.1.3 | Built for Hadoop 3 |
| Pig | 0.18.0 | Apache documents Hadoop 3 support for this release; validate the chosen runtime with fixtures during implementation |

The Pig, Hive and Hadoop versions are the targets for the adapters that are not written yet. Record the versions actually used in `runs.engine_info` (each adapter reports them with `--engine-info`).

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
| `MONGODB_URI` | MongoDB adapter | `mongodb://localhost:27017` |

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

### MongoDB

Install MongoDB Community from MongoDB's official apt repository (see the MongoDB installation guide for Ubuntu), then:

```bash
sudo systemctl start mongod        # or: mongod --dbpath ~/mongo-data --fork --logpath ~/mongod.log
mongosh --eval 'db.runCommand({ping: 1})'
```

### Hadoop, Hive and Pig

Install OpenJDK 8 and unpack the release archives under `/opt`. Set `JAVA_HOME`, `HADOOP_HOME`, `HIVE_HOME` and `PIG_HOME`, and add their `bin` directories to `PATH`. Configure Hadoop as a single-node pseudo-distributed cluster (Hadoop documentation: "Single Node Setup"), then:

```bash
hdfs namenode -format              # first time only
start-dfs.sh && start-yarn.sh
schematool -dbType derby -initSchema   # Hive metastore, first time only
```

All three Hadoop-based adapters must run in this same mode, and they must set `textinputformat.record.delimiter` to `\n` (`PARSING_RULES.md` section 11).

### Windows portable setup (development checks only)

PostgreSQL and MongoDB can also run on Windows from their official zip archives, without installation. This setup was used to verify the MongoDB pipeline:

```bash
# PostgreSQL 16.15 (postgresql-16.15-1-windows-x64-binaries.zip), unpacked to C:\Users\<you>\etl-services
pgsql/bin/initdb.exe -D pgdata -U postgres -A trust -E UTF8 --locale=C
pgsql/bin/pg_ctl.exe -D pgdata -l postgres.log -o "-p 55432 -c listen_addresses=localhost" -w start
pgsql/bin/createdb.exe -h localhost -p 55432 -U postgres nosql_final_project
# MongoDB 8.0.20 (mongodb-windows-x86_64-8.0.20.zip)
mongodb-win32-x86_64-windows-8.0.20/bin/mongod.exe --dbpath mongodata --bind_ip 127.0.0.1 --port 27018
```

with `.env`: `PGHOST=localhost`, `PGPORT=55432`, `PGUSER=postgres`, `MONGODB_URI=mongodb://127.0.0.1:27018`. `-A trust` has no password, so use it on a single-user machine only. Do not use this setup for benchmarks; use the Linux setup above.

## 5. Running

```bash
python scripts/verify_inputs.py                                  # once per benchmark session (not timed)
python -m controller.fixture_runner --pipeline MongoDB           # adapter vs. expected fixture outputs
python -m controller.validation OUTPUT_DIR --batch-size N        # check one output directory
python -m controller.compare EXPECTED_DIR ACTUAL_DIR             # row-by-row comparison, both directions
python -m pytest                                                 # offline tests; MongoDB tests skip if unavailable
```

The controller's `run` command, the PostgreSQL loader and the report come with the PostgreSQL layer.

## 6. Adapter launch commands

The controller runs `launcher + --batch-size N --output-dir DIR --input FILE ...` from the repository root. `{python}` is replaced by the controller's own interpreter. Fill in each launcher in `config/project_config.json` when the adapter exists; the controller refuses to run an adapter whose launcher is `null`.

| Pipeline | Launcher | Status |
|---|---|---|
| MongoDB | `["{python}", "-m", "pipelines.mongodb.adapter"]` | Implemented |
| Hive | `null` | Not implemented |
| Pig | `null` | Not implemented |
| MapReduce | `null` | Not implemented |

Every adapter must also support `--debug-parsed` (fixtures) and `--engine-info` (prints a JSON object with engine name, version and mode).

## 7. Optional database service configuration

compose.yaml provides PostgreSQL 16 and MongoDB 8.0 development services.
Set PGPASSWORD in .env (or the shell) and keep it private. The PostgreSQL
service creates user etl and database nosql_final_project to match .env.example.
Its default ports are 5432 and 27017, bound to localhost.

~~~bash
docker compose up -d postgres mongodb
psql -h localhost -U etl -v ON_ERROR_STOP=1 -d nosql_final_project -f database/schema.sql
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
