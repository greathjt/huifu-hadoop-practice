package com.huifu.hadoop.hdfs.join.semijoin;

import com.huifu.hadoop.hdfs.model.UserLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by chao.hu on 2016/12/19.
 */
public class UniqueHashedKeyJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new UniqueHashedKeyJob(), args);
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();



        Job job = Job.getInstance(conf);
        job.setJarByClass(UniqueHashedKeyJob.class);

        job.setMapperClass(Map.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<Text, Text, Text, NullWritable> {
        private static Set<String> sets = new HashSet<String>();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            UserLog log = UserLog.fromText(key);
            System.out.println("K[" + log.getName() + "]");
//            System.out.println("V[" + value.toString() + "]");
            sets.add(log.getName());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text output = new Text();
            for (String key : sets) {
                System.out.println("OutK[" + key + "]");
                output.set(key);
                context.write(output, NullWritable.get());
            }
        }
    }

    public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
