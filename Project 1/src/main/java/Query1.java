import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Query1 {
    public static class Query1Mapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split(",");

            if(Integer.parseInt(data[2]) >=20 && Integer.parseInt(data[2])<=50){
                int valuePair = Integer.parseInt(data[2]);
                context.write(new Text(data[0]), new IntWritable(valuePair));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        String inputPath = "hdfs://localhost:9000/project1/Customers.csv";
        String outPath = "hdfs://localhost:9000/project1/Query1_Output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(Query1Mapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
