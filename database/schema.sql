-- PostgreSQL 15+. Implements docs/DATABASE_DESIGN.md.
-- Create/connect to nosql_final_project before running this script:
--   psql -v ON_ERROR_STOP=1 -d nosql_final_project -f database/schema.sql
-- Safe to reapply to this schema version; existing table definitions are not migrated.
-- Result loading must be one transaction. Only report runs with status = 'COMPLETED'.
-- Stop the runtime timer after that transaction commits, then save elapsed_seconds.

BEGIN;

DO $$
BEGIN
    IF current_setting('server_version_num')::integer < 150000 THEN
        RAISE EXCEPTION 'This schema requires PostgreSQL 15 or later';
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS public.runs (
    run_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(30) NOT NULL
        CHECK (pipeline_name IN ('Pig', 'MapReduce', 'MongoDB', 'Hive')),
    batch_size BIGINT NOT NULL CHECK (batch_size > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'FAILED', 'COMPLETED')),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    elapsed_seconds DOUBLE PRECISION
        CHECK (elapsed_seconds >= 0 AND elapsed_seconds < 'Infinity'::double precision),
    total_records BIGINT CHECK (total_records >= 0),
    total_batches BIGINT CHECK (total_batches >= 0),
    malformed_count BIGINT CHECK (malformed_count BETWEEN 0 AND total_records),
    average_batch_size DOUBLE PRECISION
        CHECK (average_batch_size > 0 AND average_batch_size < 'Infinity'::double precision),
    input_files JSONB NOT NULL CHECK (jsonb_typeof(input_files) = 'array'),
    engine_info JSONB CHECK (jsonb_typeof(engine_info) = 'object'),
    environment JSONB CHECK (jsonb_typeof(environment) = 'object'),
    error_message TEXT,
    CHECK (status <> 'COMPLETED'
        OR (elapsed_seconds IS NOT NULL AND completed_at IS NOT NULL)),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (total_batches IS NULL OR total_records IS NULL OR (
        (total_batches = 0 AND total_records = 0 AND average_batch_size IS NULL)
        OR (total_batches > 0 AND total_records >= total_batches
            AND average_batch_size IS NOT NULL)
    )),
    -- Enforce result metadata copied from the parent run.
    UNIQUE (run_id, pipeline_name, started_at)
);

CREATE TABLE IF NOT EXISTS public.run_batches (
    run_id UUID NOT NULL REFERENCES public.runs(run_id) ON DELETE CASCADE,
    batch_id INTEGER NOT NULL CHECK (batch_id > 0),
    record_count BIGINT NOT NULL CHECK (record_count > 0),
    malformed_count BIGINT NOT NULL CHECK (malformed_count BETWEEN 0 AND record_count),
    PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS public.phase_timings (
    timing_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES public.runs(run_id) ON DELETE CASCADE,
    phase_name VARCHAR(50) NOT NULL CHECK (phase_name <> ''),
    duration_seconds DOUBLE PRECISION NOT NULL
        CHECK (duration_seconds >= 0 AND duration_seconds < 'Infinity'::double precision)
);

CREATE TABLE IF NOT EXISTS public.malformed_samples (
    run_id UUID NOT NULL REFERENCES public.runs(run_id) ON DELETE CASCADE,
    record_number BIGINT NOT NULL CHECK (record_number > 0),
    batch_id INTEGER NOT NULL,
    source_file TEXT NOT NULL CHECK (source_file <> ''),
    reason VARCHAR(20) NOT NULL
        CHECK (reason IN ('BLANK_LINE', 'BAD_STRUCTURE', 'BAD_HOST',
                          'BAD_TIMESTAMP', 'BAD_STATUS', 'BAD_BYTES')),
    -- Loader escapes U+0000 as the six literal characters \u0000 before insertion.
    raw_line TEXT NOT NULL,
    PRIMARY KEY (run_id, record_number),
    FOREIGN KEY (run_id, batch_id)
        REFERENCES public.run_batches(run_id, batch_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.q1_daily_traffic (
    run_id UUID NOT NULL REFERENCES public.runs(run_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(30) NOT NULL,
    batch_id INTEGER,
    executed_at TIMESTAMPTZ NOT NULL,
    log_date DATE NOT NULL,
    status_code INTEGER NOT NULL CHECK (status_code BETWEEN 0 AND 999),
    request_count BIGINT NOT NULL CHECK (request_count > 0),
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    FOREIGN KEY (run_id, pipeline_name, executed_at)
        REFERENCES public.runs(run_id, pipeline_name, started_at) ON DELETE CASCADE,
    FOREIGN KEY (run_id, batch_id)
        REFERENCES public.run_batches(run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, log_date, status_code)
);

CREATE TABLE IF NOT EXISTS public.q2_top_resources (
    run_id UUID NOT NULL REFERENCES public.runs(run_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(30) NOT NULL,
    batch_id INTEGER,
    executed_at TIMESTAMPTZ NOT NULL,
    rank INTEGER NOT NULL CHECK (rank BETWEEN 1 AND 20),
    resource_path TEXT COLLATE "C" NOT NULL CHECK (resource_path <> ''),
    request_count BIGINT NOT NULL CHECK (request_count > 0),
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    distinct_host_count BIGINT NOT NULL
        CHECK (distinct_host_count BETWEEN 1 AND request_count),
    FOREIGN KEY (run_id, pipeline_name, executed_at)
        REFERENCES public.runs(run_id, pipeline_name, started_at) ON DELETE CASCADE,
    FOREIGN KEY (run_id, batch_id)
        REFERENCES public.run_batches(run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, resource_path),
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, rank)
);

CREATE TABLE IF NOT EXISTS public.q3_hourly_errors (
    run_id UUID NOT NULL REFERENCES public.runs(run_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(30) NOT NULL,
    batch_id INTEGER,
    executed_at TIMESTAMPTZ NOT NULL,
    log_date DATE NOT NULL,
    log_hour INTEGER NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count BIGINT NOT NULL CHECK (error_request_count >= 0),
    total_request_count BIGINT NOT NULL CHECK (total_request_count > 0),
    error_rate NUMERIC(20,12) NOT NULL CHECK (error_rate BETWEEN 0 AND 1),
    distinct_error_hosts BIGINT NOT NULL
        CHECK (distinct_error_hosts BETWEEN 0 AND error_request_count),
    CHECK (error_request_count <= total_request_count),
    CHECK ((distinct_error_hosts = 0) = (error_request_count = 0)),
    CHECK (abs(error_rate - error_request_count::numeric / total_request_count) <= 0.000000001),
    FOREIGN KEY (run_id, pipeline_name, executed_at)
        REFERENCES public.runs(run_id, pipeline_name, started_at) ON DELETE CASCADE,
    FOREIGN KEY (run_id, batch_id)
        REFERENCES public.run_batches(run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, log_date, log_hour)
);

CREATE INDEX IF NOT EXISTS runs_completed_lookup
    ON public.runs (pipeline_name, batch_size, started_at) WHERE status = 'COMPLETED';
CREATE INDEX IF NOT EXISTS phase_timings_run_lookup ON public.phase_timings (run_id);

-- The loader checks consecutive batch IDs, batch sizes against runs.batch_size,
-- aggregate totals, the first-100 sample limit and Q2 rank/order before loading.
-- These cross-row rules are not expressible as ordinary row CHECK constraints.
-- A failure after the results commit requires recovery/cleanup; ROLLBACK cannot
-- undo an already committed result transaction.

COMMIT;

