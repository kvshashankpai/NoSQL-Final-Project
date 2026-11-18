-- Included by the adapter for --debug-parsed runs only.
debug_rows = FOREACH parsed GENERATE record_number, source_file, valid, reason,
    host, timestamp, log_date, log_hour, http_method, resource_path,
    protocol_version, status_code, bytes_transferred;
STORE debug_rows INTO '$RESULT/debug' USING JsonStorage();
