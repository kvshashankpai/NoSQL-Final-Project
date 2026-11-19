
package nosql.mapreduce.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourlyErrorDriver {

    public static void main(String[] args) throws Exception {
        // Execution mode comes from -D options (the adapter) or the Hadoop site config.
        Configuration configuration = new Configuration();
        args = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (args.length != 2) {
            System.err.println(
                "Usage: HourlyErrorDriver <parsed_input> <hourly_output>"
            );
            System.exit(2);
        }

        configuration.set("textinputformat.record.delimiter", "\n");

        Job job = Job.getInstance(configuration, "Q3 Hourly Error Analysis");

        job.setJarByClass(HourlyErrorDriver.class);

        job.setMapperClass(HourlyErrorMapper.class);
        job.setReducerClass(HourlyErrorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // One reducer aggregates all requests for each date-hour.
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
