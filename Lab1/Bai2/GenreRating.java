import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GenreRating {

    // Mapper: lấy MovieID và Rating
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

        private IntWritable movieId = new IntWritable();
        private FloatWritable rating = new FloatWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                try {
                    movieId.set(Integer.parseInt(parts[1].trim()));
                    rating.set(Float.parseFloat(parts[2].trim()));
                    context.write(movieId, rating);
                } catch (Exception e) {
                    // bỏ qua dòng lỗi
                }
            }
        }
    }

    // Reducer: tính theo genre
    public static class GenreReducer extends Reducer<IntWritable, FloatWritable, Text, Text> {

        // MovieID -> genres
        private HashMap<Integer, List<String>> movieGenres = new HashMap<>();

        // tổng và count theo genre
        private HashMap<String, Float> genreSum = new HashMap<>();
        private HashMap<String, Integer> genreCount = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            Path path = new Path("/movies.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");

                int id = Integer.parseInt(parts[0].trim());

                // 🔥 fix khoảng trắng
                String[] rawGenres = parts[2].split("\\|");
                List<String> genres = new ArrayList<>();

                for (String g : rawGenres) {
                    genres.add(g.trim());
                }

                movieGenres.put(id, genres);
            }
            br.close();
        }

        @Override
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            List<String> genres = movieGenres.get(key.get());
            if (genres == null) return;

            for (String genre : genres) {
                genreSum.put(genre, genreSum.getOrDefault(genre, 0f) + sum);
                genreCount.put(genre, genreCount.getOrDefault(genre, 0) + count);
            }
        }

        // chỉ in 1 lần → không duplicate
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (String genre : genreSum.keySet()) {

                float sum = genreSum.get(genre);
                int count = genreCount.get(genre);
                float avg = sum / count;

                context.write(
                        new Text(genre),
                        new Text("Avg: " + String.format("%.2f", avg) + ", Count: " + count)
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating");

        job.setJarByClass(GenreRating.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}