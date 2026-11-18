
package nosql.mapreduce.q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopResourcesDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: TopResourcesDriver <aggregation_input> <top20_output>"
            );
            System.exit(2);
        }

        Configuration configuration = new Configuration();

        configuration.set("mapreduce.framework.name", "local");
        configuration.set("textinputformat.record.delimiter", "\n");
        configuration.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(configuration, "Q2 Global Top 20 Resources");

        job.setJarByClass(TopResourcesDriver.class);

        job.setMapperClass(TopResourcesMapper.class);
        job.setReducerClass(TopResourcesReducer.class);

        // Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final ranked output
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // All resources must reach one reducer for global ranking
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
