package Lab1.Bai1;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1MovieRating {

    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        private IntWritable movieIdKey = new IntWritable();
        private DoubleWritable ratingValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*");
            if (parts.length < 4) return;

            try {
                int movieId = Integer.parseInt(parts[1]);
                double rating = Double.parseDouble(parts[2]);

                movieIdKey.set(movieId);
                ratingValue.set(rating);

                context.write(movieIdKey, ratingValue);
            } catch (NumberFormatException e) {
                // bo qua dong loi
            }
        }
    }

    public static class RatingReducer extends Reducer<IntWritable, DoubleWritable, Text, Text> {
        private Map<Integer, String> movieMap = new HashMap<>();

        private String maxMovie = "";
        private double maxRating = Double.MIN_VALUE;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            File file = new File("movies.txt");
            loadMovies(file);
        }

        private void loadMovies(File file) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",\\s*", 3);
                if (parts.length >= 2) {
                    try {
                        int movieId = Integer.parseInt(parts[0]);
                        String title = parts[1];
                        movieMap.put(movieId, title);
                    } catch (NumberFormatException e) {
                        // bo qua dong loi
                    }
                }
            }
            br.close();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = sum / count;
            String movieTitle = movieMap.getOrDefault(key.get(), "Unknown Movie");

            context.write(
                new Text(movieTitle),
                new Text(String.format("AverageRating: %.1f (TotalRatings: %d)", avg, count))
            );

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieTitle;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(
                    new Text("TOP MOVIE"),
                    new Text(String.format(
                        "%s is the highest rated movie with an average rating of %.1f among movies with at least 5 ratings.",
                        maxMovie, maxRating
                    ))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Bai1MovieRating <input_ratings_dir> <output_dir> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 1 - Movie Average Rating");

        job.setJarByClass(Bai1MovieRating.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[2] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
