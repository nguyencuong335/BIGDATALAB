package bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CleanReview extends EvalFunc<String> {

    private static final Set<String> stopwords = new HashSet<>();
    private final String stopwordPath;

    public CleanReview(String stopwordPath) throws IOException {
        this.stopwordPath = stopwordPath;
        if (stopwords.isEmpty()) {
            loadStopwords();
        }
    }

    private void loadStopwords() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(stopwordPath);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (!line.isEmpty()) {
                    stopwords.add(line);
                }
            }
        }
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }

        String review = input.get(0).toString().trim().toLowerCase();
        if (review.isEmpty()) {
            return "";
        }

        String[] tokens = review.split("\\s+");
        StringJoiner joiner = new StringJoiner(" ");

        for (String token : tokens) {
            token = token.trim();
            if (!token.isEmpty() && !stopwords.contains(token)) {
                joiner.add(token);
            }
        }

        return joiner.toString();
    }
}