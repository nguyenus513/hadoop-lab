import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class CategorySentimentTopWords {

    public static class MapperCS extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text outKey = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String category = "";
            if (line.contains("hotel")) category = "hotel";
            else if (line.contains("food")) category = "food";
            else if (line.contains("service")) category = "service";
            else if (line.contains("location")) category = "location";

            String sentiment = "";
            if (line.contains("positive")) sentiment = "pos";
            else if (line.contains("negative")) sentiment = "neg";

            if (category.equals("") || sentiment.equals("")) return;

            String[] words = line.split("\\s+");

            for (String w : words) {
                w = w.replaceAll("[^\\p{L}]", "");
                if (!w.isEmpty()) {
                    outKey.set(category + "_" + sentiment + "_" + w);
                    context.write(outKey, one);
                }
            }
        }
    }

    public static class ReducerCS extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, Integer> map = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            map.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            Map<String, List<Map.Entry<String, Integer>>> grouped = new HashMap<>();

            for (Map.Entry<String, Integer> e : map.entrySet()) {
                String[] parts = e.getKey().split("_", 3);
                String group = parts[0] + "_" + parts[1];

                grouped.putIfAbsent(group, new ArrayList<>());
                grouped.get(group).add(e);
            }

            for (String group : grouped.keySet()) {
                List<Map.Entry<String, Integer>> list = grouped.get(group);

                list.sort((a, b) -> b.getValue() - a.getValue());

                int count = 0;
                for (Map.Entry<String, Integer> e : list) {
                    context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
                    count++;
                    if (count == 5) break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Sentiment Top Words");

        job.setJarByClass(CategorySentimentTopWords.class);
        job.setMapperClass(MapperCS.class);
        job.setReducerClass(ReducerCS.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}