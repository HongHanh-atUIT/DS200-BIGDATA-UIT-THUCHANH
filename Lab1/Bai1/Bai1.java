import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class Bai1 {

    // MAPPER
    //<key input, value input, key output, value output>
    public static class RatingMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text movieId = new Text(); 
        private FloatWritable ratingVal = new FloatWritable();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // ratings: userID, movieID, Rating, Timestamp
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                movieId.set(parts[1]);  
                ratingVal.set(Float.parseFloat(parts[2])); 
                context.write(movieId, ratingVal);
            }
        }
    }

    // REDUCER 
    public static class RatingReducer extends Reducer<Text, FloatWritable, Text, Text> {

        // Tra cứu tên phim từ ID, MovieMap lưu ánh xạ id => title
        private Map<String, String> movieMap = new HashMap<>();

        private String bestMovie = "";
        private float maxRating = 0;

        @Override
        protected void setup(Context context) throws IOException {

            Path[] cacheFiles = context.getLocalCacheFiles();

            if (cacheFiles != null) {
                for (Path path : cacheFiles) {
                    // Đọc từng dòng
                    BufferedReader br = new BufferedReader(new FileReader(path.toString()));
                    String line;

                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts.length >= 2) {
                            movieMap.put(parts[0].trim(), parts[1].trim());
                        }
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            // Chạy cho mỗi movie ID và tính tổng, trung bình
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float avg = sum / count;

            // Lấy tên và id
            String movieId = key.toString().trim();
            String title = movieMap.containsKey(movieId) ? movieMap.get(movieId) : "Unknown";

            // Movie điểm cao nhất (>5 lần rate)
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                bestMovie = title;
            }

            context.write(new Text(title),
                    new Text("Average rating: " + avg + " (Total ratings: " + count + ")"));
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            // Sau khi reducer xong hết, chạy cleanup 1 lần để in ra best movie
            if (bestMovie.isEmpty()) {
                context.write(new Text("No movies exist"),
                        new Text("No movie with highest ratings"));
            } else {
                context.write(new Text(bestMovie),
                        new Text("is the highest rated movie with an average rating of "
                                + maxRating + " among movies with at least 5 ratings."));
            }
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        // Tạo hadoop job 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap1");

        // Chỉ định Mapper Reducer
        job.setJarByClass(Bai1.class);
        job.setJar("Bai1.jar");
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        // Output của Mapper để shuffle/sort
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // ratings_1, ratings_2
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // add movies.txt
        job.addCacheFile(new Path(args[3]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}