import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class DataCleaning {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Set<String> stopwords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0].getPath());
                BufferedReader br = new BufferedReader(new FileReader(new File(path.getName())));

                String line;
                while ((line = br.readLine()) != null) {
                    stopwords.add(line.trim());
                }
                br.close();
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            String[] words = line.split("\\s+");

            for (String word : words) {
                word = word.replaceAll("[^\\p{L}]", "");

                if (!word.isEmpty() && !stopwords.contains(word)) {
                    context.write(new Text(word), NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Cleaning");

        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(args[2]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}