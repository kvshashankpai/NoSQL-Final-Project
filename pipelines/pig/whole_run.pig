-- Read all valid rows saved by the completed batches.
SET opt.fetch false;
SET pig.auto.local.enabled false;
SET pig.temp.dir '$ROOT/tmp';
SET mapreduce.output.fileoutputformat.compress false;

saved = LOAD '$PARSED/*/part*' USING BinStorage() AS (
    host:chararray, log_date:chararray, log_hour:int, resource_path:chararray,
    status_code:int, bytes_transferred:long
);
traffic = FOREACH saved GENERATE host, log_date, log_hour, resource_path,
    status_code, (biginteger) bytes_transferred AS bytes_transferred;
