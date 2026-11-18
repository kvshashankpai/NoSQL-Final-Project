
package nosql.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParseLogsDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                "Usage: ParseLogsDriver <input> <output> <batch-size>"
            );
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int batchSize = Integer.parseInt(args[2]);

        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("textinputformat.record.delimiter", "\n");
        configuration.set("fs.defaultFS", "file:///");
        configuration.setInt("etl.batch.size", batchSize);

        Job job = Job.getInstance(configuration, "NASA Log Parsing");

        job.setJarByClass(ParseLogsDriver.class);
        job.setMapperClass(ParseLogsMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
