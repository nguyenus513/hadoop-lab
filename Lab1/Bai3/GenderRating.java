import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GenderRating {

    // Mapper: lấy MovieID -> (UserID, Rating)
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

                } catch (Exception e) {
                    // bỏ dòng lỗi
                }
            }
        }
    }

    // Reducer
    public static class GenderReducer extends Reducer<IntWritable, Text, Text, Text> {

        private HashMap<Integer, String> userGender = new HashMap<>();
        private HashMap<Integer, String> movieTitle = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            FileSystem fs = FileSystem.get(context.getConfiguration());

            // load users.txt
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fs.open(new Path("/input/users.txt"))));
            String line;

            while ((line = br1.readLine()) != null) {
                String[] parts = line.split(",");
                int userId = Integer.parseInt(parts[0].trim());
                String gender = parts[1].trim();
                userGender.put(userId, gender);
            }
            br1.close();

            // load movies.txt
            BufferedReader br2 = new BufferedReader(new InputStreamReader(fs.open(new Path("/input/movies.txt"))));

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

            float maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");

                int userId = Integer.parseInt(parts[0]);
                float rating = Float.parseFloat(parts[1]);

                String gender = userGender.get(userId);

                if (gender == null) continue;

                if (gender.equals("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equals("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            float maleAvg = (maleCount == 0) ? 0 : maleSum / maleCount;
            float femaleAvg = (femaleCount == 0) ? 0 : femaleSum / femaleCount;

            String title = movieTitle.getOrDefault(key.get(), "Unknown");

            context.write(
                    new Text(title),
                    new Text("Male: " + String.format(Locale.US, "%.2f", maleAvg)
                            + ", Female: " + String.format(Locale.US, "%.2f", femaleAvg))
            );
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating");

        job.setJarByClass(GenderRating.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}