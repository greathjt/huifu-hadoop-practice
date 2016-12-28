package com.huifu.hadoop.hdfs.sampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by chao.hu on 2016/12/28.
 */
public final class SamplerJob extends Configured implements Tool {

    /**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SamplerJob(), args);
        System.exit(res);
    }

    /**
     * The MapReduce driver - setup and launch the job.
     *
     * @param args the command-line arguments
     * @return the process exit code
     * @throws Exception if something goes wrong
     */
    public int run(final String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SamplerJob.class);

        ReservoirSamplerInputFormat.setInputFormat(job,
                TextInputFormat.class);

        ReservoirSamplerInputFormat.setNumSamples(job, 10);
        ReservoirSamplerInputFormat.setMaxRecordsToRead(job, 10000);
        ReservoirSamplerInputFormat.setUseSamplesNumberPerInputSplit(job, true);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
}
