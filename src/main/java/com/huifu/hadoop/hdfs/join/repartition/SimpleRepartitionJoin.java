package com.huifu.hadoop.hdfs.join.repartition;

import com.huifu.hadoop.hdfs.model.User;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htuple.Tuple;

import java.io.IOException;

/**
 * Created by chao.hu on 2016/12/21.
 */
public class SimpleRepartitionJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SimpleRepartitionJoin(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path logsPath = new Path(args[1]);
        Path output = new Path(args[2]);

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SimpleRepartitionJoin.class);

        MultipleInputs.addInputPath(job, usersPath, TextInputFormat, UserMap.class);
        MultipleInputs.addInputPath(job, logsPath, TextInputFormat, LogMap.class);

        return 0;
    }

    enum ValueFields {
        DATASET,
        DATA
    }

    public static final int USERS = 0;
    public static final int USER_LOGS = 1;

    public static class UserMap extends Mapper<LongWritable, Text, Text, TupleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            User user = User.fromString(value.toString());
            Tuple tuple = new Tuple();
            tuple.setInt()

            super.map(key, value, context);
        }
    }

    public static class LogMap extends Mapper<LongWritable, Text, Text, TupleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }
}
