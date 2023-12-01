import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Demo {

    public static class DetectionMapper extends Mapper<Object, Text, Text, IntWritable> {
        private String filterType;
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            filterType = context.getConfiguration().get("filter.type");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 10 && parts[10].equals(filterType)) {
                String detectionValue = parts[9].trim().isEmpty() ? " " : parts[9];
                context.write(new Text(detectionValue), one);
            }
        }
    }

    public static class PercentReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Map<Text, Integer> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int total = countMap.values().stream().mapToInt(Integer::intValue).sum();
            for (Map.Entry<Text, Integer> entry : countMap.entrySet()) {
                double percent = 100.0 * entry.getValue() / total;
                context.write(entry.getKey(), new Text(String.format("%.2f%%", percent)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("filter.type", args[2]);

        Job job = Job.getInstance(conf, "Detection Composition Analysis");
        job.setJarByClass(Demo.class);
        job.setMapperClass(DetectionMapper.class);
        job.setReducerClass(PercentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
