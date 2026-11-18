-- The adapter appends queries.pig with this batch's traffic relation.
SET opt.fetch false;
SET pig.auto.local.enabled false;
SET pig.temp.dir '$ROOT/tmp';
SET mapreduce.output.fileoutputformat.compress false;
REGISTER 'pig-helpers.jar';
DEFINE parse_line nosql.pig.ParseLine();

framed = LOAD '$INPUT' USING nosql.pig.LfTextLoader() AS (line:chararray);
parsed = FOREACH framed GENERATE FLATTEN(parse_line(line)) AS (
    record_number:long, source_file:chararray, raw_line:chararray, valid:boolean,
    reason:chararray, host:chararray, timestamp:chararray, log_date:chararray,
    log_hour:int, http_method:chararray, resource_path:chararray,
    protocol_version:chararray, status_code:int, bytes_transferred:long
);

batch_group = GROUP parsed ALL;
statistics = FOREACH batch_group {
    rejected = FILTER parsed BY valid == false;
    GENERATE COUNT_STAR(parsed) AS record_count, COUNT_STAR(rejected) AS malformed_count;
};
STORE statistics INTO '$RESULT/stats' USING JsonStorage();

rejected_rows = FILTER parsed BY valid == false;
sample_fields = FOREACH rejected_rows GENERATE record_number, source_file, reason, raw_line;
ordered_samples = ORDER sample_fields BY record_number ASC;
samples = LIMIT ordered_samples $SAMPLE_LIMIT;
STORE samples INTO '$RESULT/malformed' USING JsonStorage();

accepted = FILTER parsed BY valid == true;
saved = FOREACH accepted GENERATE host, log_date, log_hour, resource_path,
    status_code, bytes_transferred;
STORE saved INTO '$PARSED' USING BinStorage();

traffic = FOREACH saved GENERATE host, log_date, log_hour, resource_path,
    status_code, (biginteger) bytes_transferred AS bytes_transferred;
