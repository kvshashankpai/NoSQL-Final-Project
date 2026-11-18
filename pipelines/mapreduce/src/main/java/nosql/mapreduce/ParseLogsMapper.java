
package nosql.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParseLogsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private int batchSize;

    @Override
    protected void setup(Context context) {
        batchSize = context.getConfiguration().getInt("etl.batch.size", 1000000);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String stagedLine = value.toString();

        int firstTab = stagedLine.indexOf('\t');
        int secondTab = firstTab >= 0
                ? stagedLine.indexOf('\t', firstTab + 1)
                : -1;

        if (firstTab < 0 || secondTab < 0) {
            context.getCounter("PARSER", "INVALID_STAGING_RECORD").increment(1);
            return;
        }

        long recordNumber;
        String sourceFile;
        String rawLog;

        try {
            recordNumber = Long.parseLong(stagedLine.substring(0, firstTab));
            sourceFile = stagedLine.substring(firstTab + 1, secondTab);
            rawLog = stagedLine.substring(secondTab + 1).replace('\uE000', '\r');
        } catch (Exception e) {
            context.getCounter("PARSER", "INVALID_STAGING_RECORD").increment(1);
            return;
        }

        int batchId = (int) ((recordNumber - 1) / batchSize) + 1;

        ParsedRecord record = LogParser.parse(
                recordNumber,
                batchId,
                sourceFile,
                rawLog
        );

        String json = toJson(record);

        context.write(NullWritable.get(), new Text(json));

        if (record.valid) {
            context.getCounter("PARSER", "VALID_RECORDS").increment(1);
        } else {
            context.getCounter("PARSER", "MALFORMED_RECORDS").increment(1);
        }
    }

    private String toJson(ParsedRecord r) {

        StringBuilder json = new StringBuilder();

        json.append("{");
        json.append("\"record_number\":").append(r.recordNumber).append(",");
        json.append("\"batch_id\":").append(r.batchId).append(",");
        json.append("\"source_file\":\"").append(escape(r.sourceFile)).append("\",");
        json.append("\"valid\":").append(r.valid).append(",");
        json.append("\"reason\":").append(nullableString(r.reason)).append(",");

        json.append("\"host\":").append(nullableString(r.host)).append(",");
        json.append("\"timestamp\":").append(nullableString(r.timestamp)).append(",");
        json.append("\"log_date\":").append(nullableString(r.logDate)).append(",");
        json.append("\"log_hour\":").append(nullableInteger(r.logHour)).append(",");
        json.append("\"http_method\":").append(nullableString(r.httpMethod)).append(",");
        json.append("\"resource_path\":").append(nullableString(r.resourcePath)).append(",");
        json.append("\"protocol_version\":").append(nullableString(r.protocolVersion)).append(",");
        json.append("\"status_code\":").append(nullableInteger(r.statusCode)).append(",");
        json.append("\"bytes_transferred\":").append(nullableLong(r.bytesTransferred));

        json.append("}");

        return json.toString();
    }

    private String nullableString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + escape(value) + "\"";
    }

    private String nullableInteger(Integer value) {
        return value == null ? "null" : value.toString();
    }

    private String nullableLong(Long value) {
        return value == null ? "null" : value.toString();
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\': escaped.append("\\\\"); break;
                case '"': escaped.append("\\\""); break;
                case '\b': escaped.append("\\b"); break;
                case '\f': escaped.append("\\f"); break;
                case '\n': escaped.append("\\n"); break;
                case '\r': escaped.append("\\r"); break;
                case '\t': escaped.append("\\t"); break;
                default:
                    if (ch < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) ch));
                    } else {
                        escaped.append(ch);
                    }
            }
        }
        return escaped.toString();
    }
}
