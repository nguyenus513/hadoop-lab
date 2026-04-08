import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Top5Words {

    public static class MapperTop extends Mapper<Object, Text, NullWritable, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

    public static class ReducerTop extends Reducer<NullWritable, Text, Text, IntWritable> {

        private Map<String, Integer> map = new HashMap<>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                if (parts.length == 2) {
                    map.put(parts[0], Integer.parseInt(parts[1]));
                }
            }

            List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            list.sort((a, b) -> b.getValue() - a.getValue());

            int count = 0;
            for (Map.Entry<String, Integer> e : list) {
                context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
                count++;
                if (count == 5) break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 5 Words");

        job.setJarByClass(Top5Words.class);
        job.setMapperClass(MapperTop.class);
        job.setReducerClass(ReducerTop.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0])); // wordcount output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}