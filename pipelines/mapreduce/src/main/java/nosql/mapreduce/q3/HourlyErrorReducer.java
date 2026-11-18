
package nosql.mapreduce.q3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HourlyErrorReducer
        extends Reducer<Text, Text, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalRequests = 0;
        long errors = 0;

        // Only hosts that generated errors should be counted here.
        Set<String> errorHosts = new HashSet<>();

        for (Text value : values) {

            String[] parts = value.toString().split("\\|", 2);

            if (parts.length != 2) {
                context.getCounter("Q3", "INVALID_VALUES").increment(1);
                continue;
            }

            int statusCode;

            try {
                statusCode = Integer.parseInt(parts[0]);

            } catch (NumberFormatException e) {
                context.getCounter("Q3", "INVALID_STATUS_VALUES")
                       .increment(1);
                continue;
            }

            String host = parts[1];

            // Every valid mapper output represents one request.
            totalRequests++;

            // HTTP status codes from 400 through 599 are errors.
            if (statusCode >= 400 && statusCode <= 599) {

                errors++;

                if (!host.isEmpty()) {
                    errorHosts.add(host);
                }
            }
        }

        // Calculate error rate
        double errorRate = totalRequests == 0
                ? 0.0
                : (double) errors / totalRequests;

        String formattedErrorRate =
                String.format(Locale.US, "%.15f", errorRate);

        // Output:
        // total_requests|errors|error_rate|distinct_error_hosts
        outputValue.set(
                totalRequests + "|"
                + errors + "|"
                + formattedErrorRate + "|"
                + errorHosts.size()
        );

        context.write(key, outputValue);
    }
}
