package nosql.mapreduce.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DailyTrafficDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: DailyTrafficDriver <input> <output>"
            );
            System.exit(1);
        }

        Configuration configuration = new Configuration();

        // Run Hadoop locally on the Mac
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("textinputformat.record.delimiter", "\n");
        configuration.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(configuration, "Daily Traffic Analysis");

        job.setJarByClass(DailyTrafficDriver.class);

        // Mapper and Reducer
        job.setMapperClass(DailyTrafficMapper.class);
        job.setReducerClass(DailyTrafficReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Number of reducers
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
