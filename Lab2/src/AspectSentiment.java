import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AspectSentiment {

    public static class MapperAS extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text keyOut = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String aspect = "";
            if (line.contains("general")) aspect = "general";
            else if (line.contains("price")) aspect = "price";
            else if (line.contains("quality")) aspect = "quality";
            else if (line.contains("style")) aspect = "style";
            else if (line.contains("misc")) aspect = "misc";

            String senti = "";
            if (line.contains("positive")) senti = "positive";
            else if (line.contains("negative")) senti = "negative";

            if (!aspect.equals("") && !senti.equals("")) {
                keyOut.set(aspect + "_" + senti);
                context.write(keyOut, one);
            }
        }
    }

    public static class ReducerAS extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Aspect Sentiment");

        job.setJarByClass(AspectSentiment.class);
        job.setMapperClass(MapperAS.class);
        job.setReducerClass(ReducerAS.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}