import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AgeGroupRating {

    // Mapper
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable movieId = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                try {
                    int userId = Integer.parseInt(parts[0].trim());
                    int mId = Integer.parseInt(parts[1].trim());
                    float rating = Float.parseFloat(parts[2].trim());

                    movieId.set(mId);
                    context.write(movieId, new Text(userId + ":" + rating));

                } catch (Exception e) {}
            }
        }
    }

    // Reducer
    public static class AgeReducer extends Reducer<IntWritable, Text, Text, Text> {

        private HashMap<Integer, Integer> userAge = new HashMap<>();
        private HashMap<Integer, String> movieTitle = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            FileSystem fs = FileSystem.get(context.getConfiguration());

            // load users
            BufferedReader br1 = new BufferedReader(
                    new InputStreamReader(fs.open(new Path("/input/users.txt")))
            );

            String line;
            while ((line = br1.readLine()) != null) {
                String[] parts = line.split(",");
                int userId = Integer.parseInt(parts[0].trim());
                int age = Integer.parseInt(parts[2].trim());
                userAge.put(userId, age);
            }
            br1.close();

            // load movies
            BufferedReader br2 = new BufferedReader(
                    new InputStreamReader(fs.open(new Path("/input/movies.txt")))
            );

            while ((line = br2.readLine()) != null) {
                String[] parts = line.split(",");
                int movieId = Integer.parseInt(parts[0].trim());
                String title = parts[1].trim();
                movieTitle.put(movieId, title);
            }
            br2.close();
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float[] sum = new float[4];
            int[] count = new int[4];

            for (Text val : values) {
                String[] parts = val.toString().split(":");

                int userId = Integer.parseInt(parts[0]);
                float rating = Float.parseFloat(parts[1]);

                Integer age = userAge.get(userId);
                if (age == null) continue;

                int group = getAgeGroup(age);

                sum[group] += rating;
                count[group]++;
            }

            String[] result = new String[4];

            for (int i = 0; i < 4; i++) {
                if (count[i] == 0) {
                    result[i] = "NA";
                } else {
                    result[i] = String.format(Locale.US, "%.2f", sum[i] / count[i]);
                }
            }

            String title = movieTitle.getOrDefault(key.get(), "Unknown");

            context.write(
                new Text(title),
                new Text("0-18: " + result[0]
                        + ", 18-35: " + result[1]
                        + ", 35-50: " + result[2]
                        + ", 50+: " + result[3])
            );
        }

        private int getAgeGroup(int age) {
            if (age <= 18) return 0;
            else if (age <= 35) return 1;
            else if (age <= 50) return 2;
            else return 3;
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating");

        job.setJarByClass(AgeGroupRating.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(AgeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}