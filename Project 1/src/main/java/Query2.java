import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;



public class Query2 {

    public String getCustomerName(String id){
        String readLine = "";
        String c_name = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader("D:\\Acadamics WPI\\CS585 Big Data Management\\Project 1\\Data\\Customers.csv"));
            while ((readLine = br.readLine()) != null) {
                String[] customerData = readLine.split(",");

                if (customerData[0].equals(id)) {
                    c_name = customerData[1];
                }
            }
        } catch (IOException e) {}
        return c_name;
    }

    public static class Query2Mapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            String[] data = value.toString().split(",");

            context.write(new Text(data[1]), new Text(Float.parseFloat(data[2])+ ","+"1"));

        }
    }
    public static class Query2Combiner extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            float total_amount = 0;
            int total_sum = 0;

            for(Text data2: values){
                String[] map_data = data2.toString().split(",");
                total_amount += Float.parseFloat(map_data[0]);
                total_sum += Integer.parseInt(map_data[1]);
            }
            context.write(key, new Text(total_amount+","+total_sum));


        }
    }

    public static class Query2Reducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            float total_amount = 0;
            int total_sum = 0;

            for(Text data2: values){
                String[] map_data = data2.toString().split(",");
                total_amount += Float.parseFloat(map_data[0]);
                total_sum += Integer.parseInt(map_data[1]);
            }
            Query2 obj = new Query2();
            String customerName = obj.getCustomerName(key.toString());
            context.write(key, new Text(customerName+","+total_amount+","+total_sum));


        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "hdfs://localhost:9000/project1/Transactions.csv";
        String outPath = "hdfs://localhost:9000/project1/Query2_Output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.Query2Mapper.class);
        job.setOutputKeyClass(Text.class);
        job.setCombinerClass(Query2Combiner.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Query2Reducer.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
