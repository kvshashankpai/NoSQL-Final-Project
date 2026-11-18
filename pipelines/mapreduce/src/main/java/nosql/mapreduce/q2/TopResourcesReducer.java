
package nosql.mapreduce.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopResourcesReducer
        extends Reducer<Text, Text, IntWritable, Text> {

    private static class ResourceEntry {
        String path;
        long requestCount;
        long totalBytes;
        long distinctHosts;

        ResourceEntry(String path, long requestCount,
                    long totalBytes, long distinctHosts) {
            this.path = path;
            this.requestCount = requestCount;
            this.totalBytes = totalBytes;
            this.distinctHosts = distinctHosts;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<ResourceEntry> resources = new ArrayList<>();

        // Read all resource aggregates
        for (Text value : values) {

            String record = value.toString();
            String[] parts = record.split("\\t", 2);

            if (parts.length != 2) {
                context.getCounter("TOP20", "INVALID_RECORDS").increment(1);
                continue;
            }

            String resourcePath = parts[0];
            String[] metrics = parts[1].split("\\|");

            if (metrics.length != 3) {
                context.getCounter("TOP20", "INVALID_METRICS").increment(1);
                continue;
            }

            try {
                long requestCount = Long.parseLong(metrics[0]);
                long totalBytes = Long.parseLong(metrics[1]);
                long distinctHosts = Long.parseLong(metrics[2]);

                resources.add(
                    new ResourceEntry(
                        resourcePath,
                        requestCount,
                        totalBytes,
                        distinctHosts
                    )
                );

            } catch (NumberFormatException e) {
                context.getCounter("TOP20", "INVALID_NUMERIC_VALUES")
                       .increment(1);
            }
        }

        // Sort by:
        // 1. Request count descending
        // 2. Resource path ascending for ties
        Collections.sort(resources, new Comparator<ResourceEntry>() {
            @Override
            public int compare(ResourceEntry a, ResourceEntry b) {

                int countComparison =
                        Long.compare(b.requestCount, a.requestCount);

                if (countComparison != 0) {
                    return countComparison;
                }

                return compareUtf8(a.path, b.path);
            }
        });

        // Select global Top 20 and assign explicit ranks
        int limit = Math.min(20, resources.size());

        for (int i = 0; i < limit; i++) {

            ResourceEntry resource = resources.get(i);

            int rank = i + 1;

            String outputValue =
                    resource.path + "\t"
                    + resource.requestCount + "|"
                    + resource.totalBytes + "|"
                    + resource.distinctHosts;

            context.write(
                    new IntWritable(rank),
                    new Text(outputValue)
            );
        }
    }

    private static int compareUtf8(String left, String right) {
        byte[] a = left.getBytes(StandardCharsets.UTF_8);
        byte[] b = right.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(a.length, b.length);
        for (int i = 0; i < length; i++) {
            int comparison = Integer.compare(a[i] & 0xff, b[i] & 0xff);
            if (comparison != 0) return comparison;
        }
        return Integer.compare(a.length, b.length);
    }
}
