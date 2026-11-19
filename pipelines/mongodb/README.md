# MongoDB pipeline

This adapter runs the common NASA log workload through MongoDB's aggregation
framework. It streams the input in the selected logical batches, stores a
run-scoped raw collection, materializes parsed documents with a MongoDB
`$function`, and computes Q1 daily traffic, Q2 top resources, and Q3 hourly
errors with aggregation pipelines. Temporary collections are dropped after a
successful run. The controller validates the JSONL outputs and reports total
run time; `phase_timings.jsonl` also separates ingestion, parsing, and query
aggregation time.

## Start MongoDB and run from the project terminal

Install the Python driver once in the project's active environment:

```bash
python3 -m pip install -r requirements.txt
```

If MongoDB is not already running locally, start a local server with Docker:

```bash
docker run -d --name nasa-mongodb -p 27017:27017 mongo:7
```

If that container already exists but is stopped, use `docker start nasa-mongodb`.
Then launch the project menu. Add `--no-db` if PostgreSQL is not running; this
still writes and validates the run output, and is the recommended mode when
comparing engine runtimes because it avoids PostgreSQL loading overhead:

```bash
python3 controller/main.py --no-db
```

Choose **4. MongoDB**, then choose batch size 100,000, 500,000, 1,000,000, or
Custom. Default connection settings are `mongodb://localhost:27017` and
database `nasa_log_analytics`. Override them with `MONGO_URI` and
`MONGO_DATABASE` if needed.

For reproducible command-line runs:

```bash
python3 controller/main.py --pipeline mongodb --batch-size 100000 --no-db
python3 controller/main.py --pipeline mongodb --batch-size 500000 --no-db
python3 controller/main.py --pipeline mongodb --batch-size 1000000 --no-db
```

Use the same batch sizes and input files when comparing with MapReduce. Each
run writes the common result files under `results/<run-id>/`; capture the
printed elapsed seconds and the batch totals for the report. `--no-db` keeps
the benchmark usable without PostgreSQL, while the MongoDB adapter still
requires its MongoDB server. Without `--no-db`, the controller also persists
run/results in PostgreSQL, matching the existing controller measurement path.

Check driver/server connectivity and versions with:

```bash
python3 pipelines/mongodb/adapter.py --engine-info
```

MongoDB server-side JavaScript must be enabled because `$function` performs
the shared strict parsing rules inside the database. MongoDB releases that
disable server-side JavaScript cannot run this adapter as written.
