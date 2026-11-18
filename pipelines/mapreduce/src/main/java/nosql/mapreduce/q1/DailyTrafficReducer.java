package nosql.mapreduce.q1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyTrafficReducer
        extends Reducer<Text, Text, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(
            Text key,
            Iterable<Text> values,
            Context context
    ) throws IOException, InterruptedException {

        long totalRequests = 0;
        long totalBytes = 0;

        for (Text value : values) {

            String[] parts = value.toString().split("\\|");

            if (parts.length != 2) {
                context.getCounter("Q1", "INVALID_VALUES").increment(1);
                continue;
            }

            long requests = Long.parseLong(parts[0]);
            long bytes = Long.parseLong(parts[1]);

            totalRequests += requests;
            totalBytes += bytes;
        }

        // Output: request_count|total_bytes
        outputValue.set(totalRequests + "|" + totalBytes);

        context.write(key, outputValue);
    }
}