package Lab1.Bai3;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3GenderRating {

    public static class GenderMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private Map<Integer, String> userGenderMap = new HashMap<>();
        private IntWritable movieIdKey = new IntWritable();
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            File usersFile = new File("users.txt");
            loadUsers(usersFile);
        }

        private void loadUsers(File file) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Format: UserID, Gender, Age, Occupation, Zip-code
                String[] parts = line.split(",\\s*");
                if (parts.length >= 2) {
                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        String gender = parts[1].trim();
                        userGenderMap.put(userId, gender);
                    } catch (NumberFormatException e) {
                        // bo qua dong loi
                    }
                }
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Format: UserID, MovieID, Rating, Timestamp
            String[] parts = line.split(",\\s*");
            if (parts.length < 4) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                String gender = userGenderMap.get(userId);
                if (gender == null || gender.isEmpty()) return;

                movieIdKey.set(movieId);
                outputValue.set(gender + ":" + rating);

                context.write(movieIdKey, outputValue);

            } catch (NumberFormatException e) {
                // bo qua dong loi
            }
        }
    }

    public static class GenderReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Map<Integer, String> movieTitleMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            File moviesFile = new File("movies.txt");
            loadMovies(moviesFile);
        }

        private void loadMovies(File file) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Format: MovieID, Title, Genres
                String[] parts = line.split(",\\s*", 3);
                if (parts.length >= 2) {
                    try {
                        int movieId = Integer.parseInt(parts[0].trim());
                        String title = parts[1].trim();
                        movieTitleMap.put(movieId, title);
                    } catch (NumberFormatException e) {
                        // bo qua dong loi
                    }
                }
            }
            br.close();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double maleSum = 0.0;
            int maleCount = 0;

            double femaleSum = 0.0;
            int femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length != 2) continue;

                String gender = parts[0].trim();
                double rating;

                try {
                    rating = Double.parseDouble(parts[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }

                if (gender.equalsIgnoreCase("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equalsIgnoreCase("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            double maleAvg = maleCount > 0 ? maleSum / maleCount : 0.0;
            double femaleAvg = femaleCount > 0 ? femaleSum / femaleCount : 0.0;

            String movieTitle = movieTitleMap.getOrDefault(key.get(), "Unknown Movie");

            context.write(
                new Text(movieTitle),
                new Text(String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg))
            );
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai3GenderRating <input_ratings_dir> <output_dir> <movies_file> <users_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 3 - Gender Rating Analysis");

        job.setJarByClass(Bai3GenderRating.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[2] + "#movies.txt"));
        job.addCacheFile(new URI(args[3] + "#users.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}