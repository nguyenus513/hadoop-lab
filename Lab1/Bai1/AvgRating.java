import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AvgRating {

    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

        private IntWritable movieId = new IntWritable();
        private FloatWritable rating = new FloatWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            // chỉ xử lý ratings (tránh lỗi file khác)
            if (fields.length >= 3) {
                try {
                    movieId.set(Integer.parseInt(fields[1].trim()));
                    rating.set(Float.parseFloat(fields[2].trim()));
                    context.write(movieId, rating);
                } catch (Exception e) {
                    // bỏ qua dòng lỗi
                }
            }
        }
    }

    public static class RatingReducer extends Reducer<IntWritable, FloatWritable, Text, Text> {

        private HashMap<Integer, String> movieMap = new HashMap<>();
        private float maxRating = 0;
        private String maxMovie = "";

        @Override
        protected void setup(Context context) throws IOException {
            Path path = new Path("/movies.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                int id = Integer.parseInt(parts[0].trim());
                String title = parts[1].trim();
                movieMap.put(id, title);
            }
            br.close();
        }

        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float avg = sum / count;
            String title = movieMap.getOrDefault(key.get(), "Unknown");

            context.write(new Text(title),
                    new Text("Average rating: " + avg + " (Total ratings: " + count + ")"));

            // (>=5 ratings)
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (maxMovie.equals("")) {
                context.write(new Text(""),
                        new Text("No movie has at least 5 ratings."));
            } else {
                context.write(new Text(""),
                        new Text(maxMovie + " is the highest rated movie with an average rating of " + maxRating));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating");

        job.setJarByClass(AvgRating.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}