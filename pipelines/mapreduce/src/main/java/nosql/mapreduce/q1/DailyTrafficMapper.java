package nosql.mapreduce.q1;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyTrafficMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern VALID_PATTERN =
            Pattern.compile("\"valid\"\\s*:\\s*true");

    private static final Pattern DATE_PATTERN =
            Pattern.compile("\"log_date\"\\s*:\\s*\"([^\"]+)\"");

    private static final Pattern STATUS_PATTERN =
            Pattern.compile("\"status_code\"\\s*:\\s*(\\d+)");

    private static final Pattern BYTES_PATTERN =
            Pattern.compile("\"bytes_transferred\"\\s*:\\s*(\\d+)");

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String json = value.toString();

        // Ignore malformed records
        if (!VALID_PATTERN.matcher(json).find()) {
            context.getCounter("Q1", "INVALID_RECORDS_SKIPPED").increment(1);
            return;
        }

        Matcher dateMatcher = DATE_PATTERN.matcher(json);
        Matcher statusMatcher = STATUS_PATTERN.matcher(json);
        Matcher bytesMatcher = BYTES_PATTERN.matcher(json);

        // Check whether all required fields exist
        if (!dateMatcher.find()
                || !statusMatcher.find()
                || !bytesMatcher.find()) {

            context.getCounter("Q1", "MISSING_FIELDS").increment(1);
            return;
        }

        String date = dateMatcher.group(1);
        int statusCode = Integer.parseInt(statusMatcher.group(1));
        long bytesTransferred = Long.parseLong(bytesMatcher.group(1));

        // Key: date|status_code
        outputKey.set(date + "|" + statusCode);

        // Value: request_count|bytes_transferred
        outputValue.set("1|" + bytesTransferred);

        context.write(outputKey, outputValue);
    }
}
