package Lab1.Bai4;

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

public class Bai4AgeGroupRating {

    public static class AgeGroupMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private Map<Integer, String> userAgeGroupMap = new HashMap<>();
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
                if (parts.length >= 3) {
                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        int age = Integer.parseInt(parts[2].trim());
                        String ageGroup = getAgeGroup(age);
                        userAgeGroupMap.put(userId, ageGroup);
                    } catch (NumberFormatException e) {
                        // bo qua dong loi
                    }
                }
            }
            br.close();
        }

        private String getAgeGroup(int age) {
            if (age <= 18) return "0-18";
            if (age <= 35) return "18-35";
            if (age <= 50) return "35-50";
            return "50+";
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

                String ageGroup = userAgeGroupMap.get(userId);
                if (ageGroup == null || ageGroup.isEmpty()) return;

                movieIdKey.set(movieId);
                outputValue.set(ageGroup + ":" + rating);

                context.write(movieIdKey, outputValue);

            } catch (NumberFormatException e) {
                // bo qua dong loi
            }
        }
    }

    public static class AgeGroupReducer extends Reducer<IntWritable, Text, Text, Text> {
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

            double sum0_18 = 0.0;
            int count0_18 = 0;

            double sum18_35 = 0.0;
            int count18_35 = 0;

            double sum35_50 = 0.0;
            int count35_50 = 0;

            double sum50Plus = 0.0;
            int count50Plus = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length != 2) continue;

                String ageGroup = parts[0].trim();
                double rating;

                try {
                    rating = Double.parseDouble(parts[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }

                switch (ageGroup) {
                    case "0-18":
                        sum0_18 += rating;
                        count0_18++;
                        break;
                    case "18-35":
                        sum18_35 += rating;
                        count18_35++;
                        break;
                    case "35-50":
                        sum35_50 += rating;
                        count35_50++;
                        break;
                    case "50+":
                        sum50Plus += rating;
                        count50Plus++;
                        break;
                }
            }

            String avg0_18 = count0_18 > 0 ? String.format("%.2f", sum0_18 / count0_18) : "NA";
            String avg18_35 = count18_35 > 0 ? String.format("%.2f", sum18_35 / count18_35) : "NA";
            String avg35_50 = count35_50 > 0 ? String.format("%.2f", sum35_50 / count35_50) : "NA";
            String avg50Plus = count50Plus > 0 ? String.format("%.2f", sum50Plus / count50Plus) : "NA";

            String movieTitle = movieTitleMap.getOrDefault(key.get(), "Unknown Movie");

            context.write(
                new Text(movieTitle),
                new Text(String.format(
                    "0-18: %s  18-35: %s  35-50: %s  50+: %s",
                    avg0_18, avg18_35, avg35_50, avg50Plus
                ))
            );
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai4AgeGroupRating <input_ratings_dir> <output_dir> <movies_file> <users_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 4 - Age Group Rating Analysis");

        job.setJarByClass(Bai4AgeGroupRating.class);

        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

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