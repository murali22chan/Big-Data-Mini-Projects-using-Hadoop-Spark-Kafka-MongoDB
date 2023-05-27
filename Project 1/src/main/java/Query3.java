import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class Query3 {
    public static class Query3CustomerMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
            String[] cust_data = values.toString().split(",");
            context.write(new Text(cust_data[0]), new Text(cust_data[1]+","+cust_data[5]+",0,0,100"));
        }
    }
    public static class Query3TransactionMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
            String[] trans_data = values.toString().split(",");
            context.write(new Text(trans_data[1]), new Text("TSDATA,0,1,"+Float.parseFloat(trans_data[2])+","+Integer.parseInt(trans_data[3])));
        }
    }

    public static class Query3Reducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String c_name = "null";
            String c_salary = "0";
            int min_item = 100;
            int num_trans = 0;
            float total_sum = 0;
            for (Text data1 : values) {

                String[] data2 = data1.toString().split(",");

                if (!data2[0].equals("TSDATA")) {
                    c_name = data2[0];
                }
                if(!data2[1].equals("0")){
                    c_salary = data2[1];
                }
                num_trans += Integer.parseInt(data2[2]);
                total_sum += Float.parseFloat(data2[3]);

                if (Integer.parseInt(data2[4]) < min_item) {
                    min_item = Integer.parseInt(data2[4]);
                }
            }

            context.write(key, new Text(c_name +","+c_salary+","+Integer.toString(num_trans)+","+Float.toString(total_sum)+","+Integer.toString(min_item)));
        }
    }
    public static void main(String[] args) throws Exception {
        String inputpath0 = "hdfs://localhost:9000/project1/Customers.csv";
        String inputPath = "hdfs://localhost:9000/project1/Transactions.csv";
        String outPath = "hdfs://localhost:9000/project1/Query3_Output";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Query3");
        job.setJarByClass(Query3.class);
        job.setMapperClass(Query3.Query3TransactionMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(Query3.Query3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputpath0), TextInputFormat.class, Query3CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, Query3TransactionMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
