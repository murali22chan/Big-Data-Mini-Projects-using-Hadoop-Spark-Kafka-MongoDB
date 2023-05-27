import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class Query5 {
    public static class Query5Mapper
            extends Mapper<LongWritable,Text,Text,Text>{

        private final HashMap<String, String> ageSexMap = new HashMap<>();
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            String line;
            String[] arr;
            for (URI path : cacheFiles) {
                BufferedReader buf = new BufferedReader(new FileReader(path.toString()));
                while ((line = buf.readLine()) != null) {
                    arr = line.split(",");
                    int age = Integer.parseInt(arr[2]);
                    StringBuilder AS = new StringBuilder();
                    if(age >= 10 && age <= 70) {
                        if (age < 20) {
                            AS.append("[10, 20)");
                        } else if (age < 30) {
                            AS.append("[20, 30)");
                        } else if (age < 40) {
                            AS.append("[30, 40)");
                        } else if (age < 50) {
                            AS.append("[40, 50)");
                        } else if (age < 60) {
                            AS.append("[50, 60)");
                        } else {
                            AS.append("[60, 70]");
                        }
                    }
                    AS.append(",");
                    AS.append(arr[3]);
                    ageSexMap.put(arr[0], AS.toString());
                }
            }
        }

        public void map(LongWritable key, Text value, Context context
        )throws IOException, InterruptedException{
            String line = value.toString();
            String[] arr = line.split(",");
            String CID = arr[1];
            String AS = ageSexMap.get(CID);
            String transTotal =  arr[2];
            value.set(transTotal);
            context.write(new Text(AS),value);
        }
    }

    public static class Query5Reducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            float max = 10f;
            float min = 1000f;
            float sum = 0f;
            int count = 0;
            for(Text t : values){
                String[] arr = t.toString().split(",") ;
                float transValue = Float.parseFloat(arr[0]);
                count++;
                sum = sum + transValue;
                if(transValue<min){
                    min = transValue;
                }
                if(transValue>max){
                    max = transValue;
                }
            }
            float avg = sum/count;
            String string = String.format("%f,%f,%f %n", min, max, avg);
            context.write(key, new Text(string));
        }
    }


    public static void main(String[] args) throws Exception{
        String inputpath0 = "hdfs://localhost:9000/project1/Customers.csv";
        String inputPath = "hdfs://localhost:9000/project1/Transactions.csv";
        String outPath = "hdfs://localhost:9000/project1/Query5_Output";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Query5");
        job.addCacheFile(new Path("hdfs://localhost:9000/project1/Customers").toUri());

        job.setJarByClass(Query5.class);
        job.setMapperClass(Query5.Query5Mapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(Query5.Query5Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputpath0), TextInputFormat.class, Query5.Query5Mapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
