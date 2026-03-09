package Lab1.Bai2;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2GenreRating {

    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Map<Integer, String> movieGenresMap = new HashMap<>();
        private Text genreKey = new Text();
        private DoubleWritable ratingValue = new DoubleWritable();

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

                // Format: MovieID, Title, Genres
                String[] parts = line.split(",\\s*", 3);
                if (parts.length >= 3) {
                    try {
                        int movieId = Integer.parseInt(parts[0].trim());
                        String genres = parts[2].trim();
                        movieGenresMap.put(movieId, genres);
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
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                String genres = movieGenresMap.get(movieId);
                if (genres == null || genres.isEmpty()) return;

                String[] genreList = genres.split("\\|");
                for (String genre : genreList) {
                    genre = genre.trim();
                    if (!genre.isEmpty()) {
                        genreKey.set(genre);
                        ratingValue.set(rating);
                        context.write(genreKey, ratingValue);
                    }
                }

            } catch (NumberFormatException e) {
                // bo qua dong loi
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double avg = sum / count;

            context.write(
                key,
                new Text(String.format("Avg: %.2f, Count: %d", avg, count))
            );
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Bai2GenreRating <input_ratings_dir> <output_dir> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 2 - Genre Rating Analysis");

        job.setJarByClass(Bai2GenreRating.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[2] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
