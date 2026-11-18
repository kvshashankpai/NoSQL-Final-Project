-- The same queries run over each batch and the full set of accepted records.
daily_groups = GROUP traffic BY (log_date, status_code);
daily = FOREACH daily_groups GENERATE group.log_date AS log_date,
    group.status_code AS status_code, COUNT_STAR(traffic) AS request_count,
    SUM(traffic.bytes_transferred) AS total_bytes;
STORE daily INTO '$RESULT/q1' USING JsonStorage();

resources = FILTER traffic BY resource_path IS NOT NULL;
resource_groups = GROUP resources BY resource_path;
resource_stats = FOREACH resource_groups {
    hosts = resources.host;
    distinct_hosts = DISTINCT hosts;
    GENERATE group AS resource_path, COUNT_STAR(resources) AS request_count,
        SUM(resources.bytes_transferred) AS total_bytes,
        COUNT_STAR(distinct_hosts) AS distinct_host_count;
};
-- Input paths are ISO-8859-1 code points. Their Pig string order matches
-- unsigned UTF-8 byte order, including non-ASCII paths.
ordered_resources = ORDER resource_stats BY request_count DESC, resource_path ASC;
top_resources = LIMIT ordered_resources $TOP_N;
STORE top_resources INTO '$RESULT/q2' USING JsonStorage();

hour_groups = GROUP traffic BY (log_date, log_hour);
hour_counts = FOREACH hour_groups {
    errors = FILTER traffic BY status_code >= 400 AND status_code <= 599;
    error_hosts = errors.host;
    distinct_error_hosts = DISTINCT error_hosts;
    GENERATE group.log_date AS log_date, group.log_hour AS log_hour,
        COUNT_STAR(errors) AS error_request_count, COUNT_STAR(traffic) AS total_request_count,
        COUNT_STAR(distinct_error_hosts) AS distinct_error_hosts;
};
hourly = FOREACH hour_counts GENERATE log_date, log_hour, error_request_count,
    total_request_count, (double) error_request_count / total_request_count AS error_rate,
    distinct_error_hosts;
STORE hourly INTO '$RESULT/q3' USING JsonStorage();
