package nosql.mapreduce.q2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopResourcesMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text("TOP");
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        // Stage 1 format:
        // resource_path    request_count|total_bytes|distinct_host_count

        String[] parts = line.split("\\t", 2);

        if (parts.length != 2) {
            context.getCounter("TOP20", "INVALID_RECORDS").increment(1);
            return;
        }

        String resourcePath = parts[0];
        String metrics = parts[1];

        String[] metricParts = metrics.split("\\|");

        if (metricParts.length != 3) {
            context.getCounter("TOP20", "INVALID_METRICS").increment(1);
            return;
        }

        try {
            long requestCount = Long.parseLong(metricParts[0]);
            long totalBytes = Long.parseLong(metricParts[1]);
            long distinctHosts = Long.parseLong(metricParts[2]);

            // Pass the complete resource aggregate to a single reducer.
            outputValue.set(
                    resourcePath + "\t"
                    + requestCount + "|"
                    + totalBytes + "|"
                    + distinctHosts
            );

            context.write(outputKey, outputValue);

        } catch (NumberFormatException e) {
            context.getCounter("TOP20", "INVALID_NUMERIC_VALUES").increment(1);
        }
    }
}