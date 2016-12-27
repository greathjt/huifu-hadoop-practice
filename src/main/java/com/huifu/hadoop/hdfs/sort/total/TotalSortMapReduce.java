package com.huifu.hadoop.hdfs.sort.total;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by chao.hu on 2016/12/16.
 */
public class TotalSortMapReduce extends Configured implements Tool {
    /**
     * Execution: hadoop jar ./huifu-hadoop-practice-1.0.0-SNAPSHOT-shaded.jar
     * com.huifu.hadoop.hdfs.ReplicatedJoin join/user.txt join/logs.txt output
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TotalSortMapReduce(), args);
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path logsPath = new Path(args[0]);
        Path partitionFile = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TotalSortMapReduce.class);

        int numReducers = 2;
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>
                (0.2, 10000, 10);
        InputSampler.writePartitionFile(job, sampler);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);

        FileInputFormat.setInputPaths(job, logsPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setNumReduceTasks(numReducers);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
