import java.net.URI;
import java.util.*;
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class Query4 {
    public static class Query4Mapper
            extends Mapper<LongWritable,Text,Text,Text>{

        private final HashMap<String, String> countryMap = new HashMap<>();
        public void setup(Context context) throws IOException, InterruptedException {
            BufferedReader buf = null;
            URI[] cacheFiles = context.getCacheFiles();
            String line;
            String[] arr;
            for (URI path : cacheFiles) {
                buf = new BufferedReader(new FileReader(path.toString()));
                while ((line = buf.readLine()) != null) {
                    arr = line.split(",");
                    countryMap.put(arr[0], arr[4]);
                }

            }

        }

        public void map(LongWritable key, Text value, Context context
        )throws IOException, InterruptedException{
            String line = value.toString();
            String[] arr = line.split(",");
            String CID = arr[1];
            String countryCode = countryMap.get(CID);
            String transTotal =  arr[2];
            value.set(CID+","+transTotal);
            context.write(new Text(countryCode),value);
        }
    }

    public static class Query4Reducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            float max = 10;
            float min = 1000;
            Set<String> cusList = new HashSet<String>();
            for(Text t : values){
                String[] arr = t.toString().split(",") ;
                String CID = arr[0];
                cusList.add(CID);
                float transValue = Float.valueOf(arr[1]);
                if(transValue<min){
                    min = transValue;
                }
                if(transValue>max){
                    max = transValue;
                }
            }
            int sum = cusList.size();
            String string = String.format("%d,%f,%f %n", sum, min, max);
            context.write(key, new Text(string));
        }
    }


    public static void main(String[] args) throws Exception {
        String inputpath0 = "hdfs://localhost:9000/project1/Customers.csv";
        String inputPath = "hdfs://localhost:9000/project1/Transactions.csv";
        String outPath = "hdfs://localhost:9000/project1/Query4_Output";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Query4");
        job.addCacheFile(new Path("hdfs://localhost:9000/project1/Customers").toUri());
        job.setJarByClass(Query4.class);
        job.setMapperClass(Query4.Query4Mapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(Query4.Query4Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputpath0), TextInputFormat.class, Query4Mapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}