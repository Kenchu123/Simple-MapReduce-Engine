import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class Join {

    public static class GenericMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, Integer> columnMap = new HashMap<>();
        private boolean headerProcessed = false;
        private String keyIndex;
        private String datasetPrefix;

        protected void setup(Context context, String keyField, String datasetPrefix) {
            this.keyIndex = context.getConfiguration().get(keyField);
            this.datasetPrefix = datasetPrefix;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (Integer.valueOf(keyIndex) < parts.length) {
                context.write(new Text(parts[Integer.valueOf(keyIndex)]), new Text(datasetPrefix + ',' + value.toString()));
            }
        }
    }

    public static class Dataset1Mapper extends GenericMapper {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context, "key.field1", "D1\t");
        }
    }

    public static class Dataset2Mapper extends GenericMapper {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context, "key.field2", "D2\t");
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniquePairs = new HashSet<>();
            Set<String> uniqueData = new HashSet<>(); // Set to store unique data
            List<Text> cache = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                uniquePairs.add(parts[0]);
                cache.add(new Text(val));
            }
            
            if (uniquePairs.size() == 2) {
                for (Text val : cache) {
                    int commaIndex = val.toString().indexOf(',');
                    if (commaIndex != -1) {
                        String data = val.toString().substring(commaIndex + 1);
                        if (uniqueData.add(data)) { // Check if data is unique
                            context.write(new Text(data), null);
                        }
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("Usage: Join <input1> <input2> <output> <keyIndex1> <keyIndex2>");
            System.exit(2);
        }

        conf.set("key.field1", otherArgs[3]);
        conf.set("key.field2", otherArgs[4]);

        Job job = Job.getInstance(conf, "Join D1 and D2");
        job.setJarByClass(Join.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Dataset1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Dataset2Mapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
