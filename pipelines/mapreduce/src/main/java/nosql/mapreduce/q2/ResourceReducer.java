package nosql.mapreduce.q2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResourceReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long requestCount = 0;
        long totalBytes = 0;
        Set<String> uniqueHosts = new HashSet<>();

        for (Text value : values) {
            String[] parts = value.toString().split("\\|", 2);

            if (parts.length != 2) {
                context.getCounter("Q2", "INVALID_VALUES").increment(1);
                continue;
            }

            String host = parts[0];

            long bytes;
            try {
                bytes = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                context.getCounter("Q2", "INVALID_BYTE_VALUES").increment(1);
                continue;
            }

            requestCount++;
            totalBytes += bytes;

            if (!host.isEmpty()) {
                uniqueHosts.add(host);
            }
        }

        context.write(
                key,
                new Text(requestCount + "|" + totalBytes + "|" + uniqueHosts.size())
        );
    }
}
