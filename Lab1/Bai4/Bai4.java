import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class Bai4 {

    // MAPPER
    //<key input, value input, key output, value output>
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> userMap = new HashMap<>(); // userID -> age group

        private String getAgeGroup(int age) {
            if (age <= 18) return "0-18";
            else if (age <= 35) return "18-35";
            else if (age <= 50) return "35-50";
            else return "50+";
        }

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
                            
                            if (parts.length >= 3){
                                userMap.put(parts[0].trim(), parts[2].trim());
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
                String age = userMap.get(userID);
                if (age != null){
                    String ageGroup = getAgeGroup(Integer.parseInt(age));
                    context.write(new Text(parts[1].trim()), new Text(ageGroup + ":" + rating));
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

            float sum_0 = 0, sum_1 = 0, sum_2 = 0, sum_3 = 0; //0-18, 18-35, 35-50, 50+
            int count_0 = 0, count_1 = 0, count_2 = 0, count_3 = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String age = parts[0];
                float rating = Float.parseFloat(parts[1]);

                if (age.equals("0-18")) { 
                    sum_0 += rating;
                    count_0++;
                } else if (age.equals("18-35")) {
                    sum_1 += rating;
                    count_1++;
                } else if (age.equals("35-50")) {
                    sum_2 += rating;
                    count_2++;
                } else if (age.equals("50+")) {
                    sum_3 += rating;
                    count_3++;
                }
            }

            float avg_0 = count_0 > 0 ? sum_0 / count_0 : 0;
            float avg_1 = count_1 > 0 ? sum_1 / count_1 : 0;
            float avg_2 = count_2 > 0 ? sum_2 / count_2 : 0;
            float avg_3 = count_3 > 0 ? sum_3 / count_3 : 0;

            String result_0 = count_0 > 0 ? String.valueOf(avg_0) : "NA";
            String result_1 = count_1 > 0 ? String.valueOf(avg_1) : "NA";
            String result_2 = count_2 > 0 ? String.valueOf(avg_2) : "NA";
            String result_3 = count_3 > 0 ? String.valueOf(avg_3) : "NA";

            // Lấy tên movies
            String movieId = key.toString().trim();
            String title = movieMap.containsKey(movieId) ? movieMap.get(movieId) : "Unknown";

            context.write(new Text(title),
                    new Text("0-18: " + result_0 + "\t18-35: " + result_1
                            + "\t35-50: " + result_2 + "\t50+: " + result_3));
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        // Tạo hadoop job 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BaiTap4");

        // Chỉ định Mapper Reducer
        job.setJarByClass(Bai4.class);
        job.setJar("Bai4.jar");
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