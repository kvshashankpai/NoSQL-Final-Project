package nosql.mapreduce.q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ResourceDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: ResourceDriver <input> <output>");
            System.exit(2);
        }

        Configuration configuration = new Configuration();

        configuration.set("mapreduce.framework.name", "local");
        configuration.set("textinputformat.record.delimiter", "\n");
        configuration.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(configuration, "Q2 Resource Aggregation");

        job.setJarByClass(ResourceDriver.class);

        job.setMapperClass(ResourceMapper.class);
        job.setReducerClass(ResourceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
