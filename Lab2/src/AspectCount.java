import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AspectCount {

    public static class MapperAspect extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text aspect = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            if (line.contains("general")) {
                aspect.set("general");
                context.write(aspect, one);
            }

            if (line.contains("price")) {
                aspect.set("price");
                context.write(aspect, one);
            }

            if (line.contains("quality")) {
                aspect.set("quality");
                context.write(aspect, one);
            }

            if (line.contains("style")) {
                aspect.set("style");
                context.write(aspect, one);
            }

            if (line.contains("miscellaneous")) {
                aspect.set("misc");
                context.write(aspect, one);
            }
        }
    }

    public static class ReducerAspect extends Reducer<Text, IntWritable, Text, IntWritable> {

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
        Job job = Job.getInstance(conf, "Aspect Count");

        job.setJarByClass(AspectCount.class);
        job.setMapperClass(MapperAspect.class);
        job.setReducerClass(ReducerAspect.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}