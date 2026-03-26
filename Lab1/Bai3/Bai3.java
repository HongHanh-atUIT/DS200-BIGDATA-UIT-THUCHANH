import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class Bai3 {

    // MAPPER
    //<key input, value input, key output, value output>
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> userMap = new HashMap<>(); // userID -> gender
        private Text gender = new Text(); 
        private Text ratingVal = new Text();

        @Override
        protected void setup(Context context) throws IOException{
            Path[] cacheFiles = context.getLocalCacheFiles();

            if (cacheFiles != null) {
                for (Path path : cacheFiles){
                    if(path.getName().contains("users")){
                        BufferedReader br = new BufferedReader(new FileReader(path.toString()));
                        String line;

                        while((line = br.readLine()) != null){
                            String[] parts = line.split(",");
                            
                            if (parts.length >= 2){
                                userMap.put(parts[0].trim(), parts[1].trim());
                            }
                        }
                        br.close();
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // ratings: userID, movieID, Rating, Timestamp
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String userID = parts[0].trim();
                float rating = Float.parseFloat(parts[2]);

                //Tra genders
                String gender = userMap.get(userID);
                if (gender != null){
                    context.write(new Text(parts[1].trim()), new Text(gender + ":" + rating));
                }
            }
        }
    }

    // REDUCER 
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> movieMap = new HashMap<>();

         @Override
        protected void setup(Context context) throws IOException {

            Path[] cacheFiles = context.getLocalCacheFiles();

            if (cacheFiles != null) {
                for (Path path : cacheFiles) {
                    if(path.getName().contains("movies")){
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
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float sumM = 0, sumF = 0;
            int countM = 0, countF = 0;

            // Chạy cho mỗi gender và tính tổng, trung bình
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String gender = parts[0];
                float rating = Float.parseFloat(parts[1]);

                if (gender.equals("M")) { 
                    sumM += rating;
                    countM++;
                } else if (gender.equals("F")) {
                    sumF += rating;
                    countF++;
                }
            }

            float avgM = countM > 0 ? sumM / countM : 0;
            float avgF = countF > 0 ? sumF / countF : 0;

            String maleResult = countM > 0 ? String.valueOf(avgM) : "N/A";
            String femaleResult = countF > 0 ? String.valueOf(avgF) : "N/A";

            // Lấy tên movies
            String movieId = key.toString().trim();
            String title = movieMap.containsKey(movieId) ? movieMap.get(movieId) : "Unknown";

            context.write(new Text(title),
                    new Text("Male: " + maleResult + ", Female: " + femaleResult));
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        // Tạo hadoop job 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap3");

        // Chỉ định Mapper Reducer
        job.setJarByClass(Bai3.class);
        job.setJar("Bai3.jar");
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        // Output của Mapper để shuffle/sort
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ratings_1, ratings_2
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // add movies.txt
        job.addCacheFile(new Path(args[3]).toUri()); //users
        job.addCacheFile(new Path(args[4]).toUri()); //movies

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}