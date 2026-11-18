package nosql.mapreduce.q3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HourlyErrorMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern VALID_PATTERN =
            Pattern.compile("\"valid\"\\s*:\\s*true");

    private static final Pattern DATE_PATTERN =
            Pattern.compile("\"log_date\"\\s*:\\s*\"([^\"]*)\"");

    private static final Pattern HOUR_PATTERN =
            Pattern.compile("\"log_hour\"\\s*:\\s*(\\d+)");

    private static final Pattern STATUS_PATTERN =
            Pattern.compile("\"status_code\"\\s*:\\s*(\\d+)");

    private static final Pattern HOST_PATTERN =
            Pattern.compile("\"host\"\\s*:\\s*\"([^\"]*)\"");

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String json = value.toString();

        // Only process valid parsed records
        if (!VALID_PATTERN.matcher(json).find()) {
            context.getCounter("Q3", "INVALID_OR_SKIPPED_RECORDS")
                   .increment(1);
            return;
        }

        Matcher dateMatcher = DATE_PATTERN.matcher(json);
        Matcher hourMatcher = HOUR_PATTERN.matcher(json);
        Matcher statusMatcher = STATUS_PATTERN.matcher(json);
        Matcher hostMatcher = HOST_PATTERN.matcher(json);

        if (!dateMatcher.find()
                || !hourMatcher.find()
                || !statusMatcher.find()
                || !hostMatcher.find()) {

            context.getCounter("Q3", "MISSING_REQUIRED_FIELDS")
                   .increment(1);
            return;
        }

        String date = dateMatcher.group(1);
        int hour;
        int statusCode;
        String host = hostMatcher.group(1);

        try {
            hour = Integer.parseInt(hourMatcher.group(1));
            statusCode = Integer.parseInt(statusMatcher.group(1));

        } catch (NumberFormatException e) {
            context.getCounter("Q3", "INVALID_NUMERIC_FIELDS")
                   .increment(1);
            return;
        }

        if (hour < 0 || hour > 23) {
            context.getCounter("Q3", "INVALID_HOUR")
                   .increment(1);
            return;
        }

        // Date + zero-padded hour ensures chronological sorting
        String groupKey = date + "|" + String.format("%02d", hour);

        // Each record contributes:
        // status_code|host
        outputKey.set(groupKey);
        outputValue.set(statusCode + "|" + host);

        context.write(outputKey, outputValue);
    }
}
