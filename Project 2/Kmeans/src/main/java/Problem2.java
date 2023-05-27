

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Problem2
{
    static List<int[]> center = new ArrayList<>();

    public static int[] convertarray(String text)
    {
        String[] array = text.split(",");
        int[] res = new int[array.length];
        for (int i = 0; i < res.length; i++)
            res[i] = Integer.parseInt(array[i]);
        return res;
    }


    public static long diff(int[] p1, int[] p2)
    {
        long x = p1[0] - p2[0];
        long y = p1[1] - p2[1];
        return (x * x) + (y * y);
    }

    public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            int[] p = convertarray(value.toString());
            int c = 0;
            for (int i = 0; i < center.size(); i++)
                if (diff(p, center.get(i)) < diff(p, center.get(c)))
                    c = i;
            context.write(new LongWritable(c), value);
        }
    }

    public static class Reducer1 extends Reducer<LongWritable, Text, LongWritable, Text>
    {
        @Override
        public void reduce(LongWritable key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException
        {
            long x = 0L;
            long y = 0L;
            int count=0;
            for (Text text : value) {
                int[] t = convertarray(text.toString());
                x += t[0];
                y += t[1];
                count++;
            }
            x = x /count;
            y = y /count;
            context.write(key, new Text(x + "," + y));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Random random= new Random();
        for (int i = 0; i < 10; i++)
            center.add(new int[]{(random.nextInt(10001-1)+1), (random.nextInt(10001-1)+1)});

        int i = 0;
        while (i++ < 6)
        {
            Configuration conf = new Configuration();
            Path path = new Path("hdfs://localhost:9000/cs585/outputP2");
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path))
                fs.delete(path, true);
            Job job = Job.getInstance(conf, "Problem2");
            job.setJarByClass(Problem2.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);
            job.setMapperClass(Problem2.Mapper1.class);
            job.setReducerClass(Problem2.Reducer1.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/cs585/DB1.txt"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/cs585/outputP2"));
            job.waitForCompletion(true);
            URI uri = new URI("hdfs://localhost:9000/cs585/outputP2/part-r-00000");
            FileSystem hdfs = FileSystem.get(uri,conf);
            InputStream in = hdfs.open(new Path(uri));
            String output = IOUtils.toString(in, StandardCharsets.UTF_8);
            boolean w = true;
            for (String line : output.split("\n"))
            {
                int centerId = Integer.parseInt(line.split("\t")[0]);
                int[] t = convertarray(line.split("\t")[1]);
                if( w && (center.get(centerId)[0] == t[0] && center.get(centerId)[1] == t[1])){
                    w=true;
                }
                else {
                    w=false;
                }
                center.set(centerId, t);
            }
            if (w)
                break;
        }


    }
}
