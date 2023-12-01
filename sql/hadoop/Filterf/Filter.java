import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Filter extends Mapper<Object, Text, NullWritable, Text> {

    private Pattern pattern;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize your pattern here using context configuration
        String regex = context.getConfiguration().get("filter.regex");
        pattern = Pattern.compile(regex);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.find()) {
            // Write only the value (the line) without the key
            context.write(NullWritable.get(), value);
        }
    }

    public static class PassThroughReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Simply write all values without changing them
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Filter <input path> <output path> <regex>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("filter.regex", args[2]);

        Job job = Job.getInstance(conf, "Regex Filter");
        job.setJarByClass(Filter.class);
        job.setMapperClass(Filter.class);
        job.setReducerClass(PassThroughReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
