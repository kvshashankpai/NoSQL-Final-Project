# Pig pipeline

This adapter uses the shared input order, record numbering and output contract.
Pig parses and aggregates one batch at a time on Hadoop MapReduce, then
recomputes all three queries across the valid records saved by every batch.
The Python adapter stages the raw records, starts Pig and packages the results.

## Mac setup

Use the existing Hadoop 3.4.3 and Java 11 described in the environment report.
Unpack the [Pig 0.18.0 Hadoop 3 binary](https://archive.apache.org/dist/pig/pig-0.18.0/)
(`pig-0.18.0.tar.gz`) into your software directory. Run the following from the
repository root. Change the installation paths if your checkout uses different
locations.

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
export HADOOP_HOME="$HOME/software/hadoop-3.4.3"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PIG_HOME="$HOME/software/pig-0.18.0"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$PATH"

/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

python -m pipelines.pig.adapter --engine-info
hdfs getconf -confKey fs.defaultFS
hdfs dfs -ls /tmp
yarn node -list
bash pipelines/pig/build.sh
```

The Mac's `fs.defaultFS` should report `hdfs://localhost:9000`, and
`mapreduce.framework.name` in `mapred-site.xml` should be `yarn`. The Hadoop
user must be able to create its own directory under HDFS `/tmp`. If the
existing cluster is stopped, start it with `start-dfs.sh` and `start-yarn.sh`
using the configured installation. Use the existing NameNode data directory.

## Fixtures

Run one small fixture first and confirm that the Pig application completes in
YARN. Then run all five fixture groups.

```bash
python -m controller.fixture_runner --pipeline Pig --fixture query
python -m controller.fixture_runner --pipeline Pig
```

For troubleshooting, run the adapter into a new directory so its logs survive.
Use `--debug-parsed` for small fixtures. A failing run prints its HDFS work path
and leaves the engine log in `OUTPUT_DIR/_work/engine.log`. After inspecting a
failed run, remove its specific HDFS work directory to reclaim space.

```bash
python -m pipelines.pig.adapter --input tests/fixtures/parsing/input/cases.log \
  --batch-size 10 --output-dir data/work/pig-parsing --debug-parsed
```

## PostgreSQL run and report

Start the existing PostgreSQL 16 service if needed. Set the connection variables
from `.env.example` in your shell or in a local `.env`, using the credentials
for the existing project database. The following `psql` command uses your
shell's PostgreSQL settings.

```bash
psql -v ON_ERROR_STOP=1 -d nosql_final_project -f database/schema.sql
```

For a small database run, record the fixture's checksum in a separate
verification directory and use that same directory when launching Pig.

```bash
python -m scripts.verify_inputs --input tests/fixtures/query/input/query.log \
  --out-dir data/work/pig-verification
python -m pipelines.pig.run --input tests/fixtures/query/input/query.log \
  --batch-size 3 --verification-dir data/work/pig-verification
```

For the full dataset, place the uncompressed files at the paths in the shared
config, record their checksums once and run Pig with the configured defaults.
Keep enough disk space for one staged batch and Hadoop's parsed data and
temporary files. Successful adapter runs clean up their HDFS workspace.

```bash
python -m scripts.verify_inputs
python -m pipelines.pig.run
```

The elapsed time starts before the input read and includes staging, HDFS
transfers, Pig execution, validation and the PostgreSQL results commit. Input
hashing, completion publication and report reads are outside that interval.
The runner prints the stored whole-run results and batch statistics.

```bash
python -m pipelines.pig.run --report RUN_UUID
python -m pipelines.pig.run --recover RUN_UUID
```

Use recovery only for a run left pending by an interrupted controller. It
checks the run's PostgreSQL session lock before touching it and uses the exact
elapsed time from the local completion record when available. Keep the matching
`runs/RUN_UUID` directory until recovery or publication completes.
