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

import java.io.IOException;
import java.util.*;

public class step2
{
    public static int findsection(int[] point)
    {
        point[0] = (point[0]-1) / 50;
        point[1] = (point[1]-1) / 50;
        return point[0] + point[1] * 200;
    }
    public static int[] convertarray(String text)
    {
        String[] array = text.split(",");
        int[] res = new int[array.length];
        for (int i = 0; i < res.length; i++)
            res[i] = Integer.parseInt(array[i]);
        return res;
    }
    public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            int sec = findsection(convertarray(value.toString()));
            context.write(new LongWritable(sec), value);
        }
    }
    public static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            Set<Integer> sec = new HashSet<>();
            String str = value.toString();
            String[] points = str.split(",");
            int[] p = new int[4];
            for (int i = 0; i < 4; i++)
                p[i] = Integer.parseInt(points[i]);
            sec.add(findsection(new int[]{p[0], p[1]}));
            sec.add(findsection(new int[]{p[0] + p[2], p[1]}));
            sec.add(findsection(new int[]{p[0], p[1] + p[3]}));
            sec.add(findsection(new int[]{p[0] + p[2], p[1] + p[3]}));
            for (int bid : sec)
                context.write(new LongWritable(bid), value);
        }
    }
    public static class Reducer1 extends Reducer<LongWritable, Text, Text, Text>
    {
        @Override
        public void reduce(LongWritable key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException
        {
            List<int[]> p = new ArrayList<>();
            List<int[]> r = new ArrayList<>();
            for (Text i : (Iterable<Text>) value)
            {
                int len = i.toString().split(",").length;
                if (len == 2)
                    p.add(convertarray(i.toString()));
                else
                    r.add(convertarray(i.toString()));
            }
            for (int[] i : r)
                for (int[] j : p)
                {
                    if ((j[0] >= i[0] && j[0] <= i[0] + i[2])
                            && (j[1] >= i[1] && j[1] <= i[1] + i[3]))
                    {
                        String res = "(" + i[0] + "," + i[1] + "," + i[2] + "," + i[3] + "),("
                                + j[0] + "," + j[1] + ")";
                        context.write(new Text(res), new Text());
                    }
                }
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(step2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://localhost:9000/cs585/DB1.txt"),
                TextInputFormat.class, step2.Mapper1.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://localhost:9000/cs585/DB2.txt"),
                TextInputFormat.class, step2.Mapper2.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/cs585/outputstep2.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}