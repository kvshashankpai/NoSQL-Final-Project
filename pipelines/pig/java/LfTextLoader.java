package nosql.pig;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.builtin.TextLoader;

/** Reads only LF as a delimiter on already framed, UTF-8 staged input. */
public final class LfTextLoader extends TextLoader {
    @Override
    public void setLocation(String location, Job job) throws IOException {
        super.setLocation(location, job);
        job.getConfiguration().set("textinputformat.record.delimiter", "\n");
    }

    @Override
    public InputFormat<LongWritable, Text> getInputFormat() {
        return new TextInputFormat();
    }
}
