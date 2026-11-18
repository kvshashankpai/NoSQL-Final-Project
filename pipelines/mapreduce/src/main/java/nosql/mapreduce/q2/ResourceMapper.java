package nosql.mapreduce.q2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ResourceMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern VALID_PATTERN =
        Pattern.compile("\"valid\"\\s*:\\s*true");

    private static final Pattern HOST_PATTERN =
            Pattern.compile("\"host\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");

    private static final Pattern RESOURCE_PATTERN =
            Pattern.compile("\"resource_path\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");

    private static final Pattern BYTES_PATTERN =
            Pattern.compile("\"bytes_transferred\"\\s*:\\s*(\\d+)");

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String json = value.toString();

        // Ignore malformed/invalid records
        if (!VALID_PATTERN.matcher(json).find()) {
            context.getCounter("Q2", "INVALID_RECORDS_SKIPPED").increment(1);
            return;
        }

        Matcher hostMatcher = HOST_PATTERN.matcher(json);
        Matcher resourceMatcher = RESOURCE_PATTERN.matcher(json);
        Matcher bytesMatcher = BYTES_PATTERN.matcher(json);

        // Check required fields
        if (!hostMatcher.find()
                || !resourceMatcher.find()
                || !bytesMatcher.find()) {

            context.getCounter("Q2", "MISSING_FIELDS").increment(1);
            return;
        }

        String host = unescapeJson(hostMatcher.group(1));
        String resource = unescapeJson(resourceMatcher.group(1));
        long bytesTransferred = Long.parseLong(bytesMatcher.group(1));

        // Exclude missing or empty resource paths
        if (resource == null || resource.trim().isEmpty()) {
            context.getCounter("Q2", "MISSING_RESOURCE_PATH").increment(1);
            return;
        }

        outputKey.set(resource);
        outputValue.set(host + "|" + bytesTransferred);

        context.write(outputKey, outputValue);
    }

    private static String unescapeJson(String value) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '\\' && i + 1 < value.length()) {
                char escaped = value.charAt(++i);
                switch (escaped) {
                    case '"': result.append('"'); break;
                    case '\\': result.append('\\'); break;
                    case 'n': result.append('\n'); break;
                    case 'r': result.append('\r'); break;
                    case 't': result.append('\t'); break;
                    default: result.append('\\').append(escaped);
                }
            } else result.append(ch);
        }
        return result.toString();
    }
}
