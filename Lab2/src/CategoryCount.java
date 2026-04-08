import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class CategoryCount {

    public static class MapperCategory extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text category = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            // xử lý chắc chắn match được
            if (line.contains("hotel")) {
                category.set("hotel");
                context.write(category, one);
            }

            if (line.contains("food")) {
                category.set("food");
                context.write(category, one);
            }

            if (line.contains("service")) {
                category.set("service");
                context.write(category, one);
            }

            if (line.contains("location")) {
                category.set("location");
                context.write(category, one);
            }
        }
    }

    public static class ReducerCategory extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Count");

        job.setJarByClass(CategoryCount.class);
        job.setMapperClass(MapperCategory.class);
        job.setReducerClass(ReducerCategory.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}