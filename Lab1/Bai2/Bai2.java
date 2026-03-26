import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class Bai2 {

    // MAPPER
    //<key input, value input, key output, value output>
    public static class RatingMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Map<String, String> movieMap = new HashMap<>(); // movieID -> genres
        private Text genre = new Text(); 
        private FloatWritable ratingVal = new FloatWritable();

        @Override
        protected void setup(Context context) throws IOException{
            Path[] cacheFiles = context.getLocalCacheFiles();

            if (cacheFiles != null) {
                for (Path path : cacheFiles){
                    BufferedReader br = new BufferedReader(new FileReader(path.toString()));
                    String line;

                    while((line = br.readLine()) != null){
                        String[] parts = line.split(",");
                        
                        if (parts.length >= 3){
                            movieMap.put(parts[0].trim(), parts[2].trim());
                        }
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // ratings: userID, movieID, Rating, Timestamp
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String movieID = parts[1].trim();
                float rating = Float.parseFloat(parts[2]);

                //Tra genres
                String genres = movieMap.get(movieID);
                if (genres != null){
                    String[] genreList = genres.split("\\|");
                    for (String g : genreList){
                        genre.set(g.trim());
                        ratingVal.set(rating);
                        context.write(genre,ratingVal);
                    }
                }
            }
        }
    }

    // REDUCER 
    public static class RatingReducer extends Reducer<Text, FloatWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            // Chạy cho mỗi genre và tính tổng, trung bình
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float avg = sum / count;

            // Lấy genre
            String genre = key.toString().trim();

            context.write(new Text(genre),
                    new Text("Avg: " + avg + ", Count: " + count));
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        // Tạo hadoop job 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap2");

        // Chỉ định Mapper Reducer
        job.setJarByClass(Bai2.class);
        job.setJar("Bai2.jar");
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