package com.huifu.hadoop.hdfs.join.repartition;

import com.google.common.collect.Lists;

import com.huifu.hadoop.hdfs.model.User;
import com.huifu.hadoop.hdfs.model.UserLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htuple.ShuffleUtils;
import org.htuple.Tuple;

import java.io.IOException;
import java.util.List;

/**
 * Created by chao.hu on 2016/12/22.
 */
public class StreamingRepartitionJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StreamingRepartitionJoin(), args);
        System.exit(res);
    }

    enum KeyFields {
        USER,
        DATASET
    }

    enum ValueFields {
        DATASET,
        DATA
    }

    public static final int USERS = 0;
    public static final int USER_LOGS = 1;

    @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path logsPath = new Path(args[1]);
        Path output = new Path(args[2]);

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(StreamingRepartitionJoin.class);


        MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
        MultipleInputs.addInputPath(job, logsPath, TextInputFormat.class, LogMap.class);
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        ShuffleUtils.configBuilder()
                .useNewApi()
                .setPartitionerIndices(KeyFields.USER)
                .setSortIndices(KeyFields.USER, KeyFields.DATASET)
                .setGroupIndices(KeyFields.USER)
                .configure(job.getConfiguration());

        output.getFileSystem(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(job.getConfiguration()).delete(output, true);

        job.setReducerClass(Reduce.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class UserMap extends Mapper<LongWritable, Text, Tuple, Tuple> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            User user = User.fromString(value.toString());

            Tuple outputKey = new Tuple();
            outputKey.setInt(KeyFields.DATASET, USERS);
            outputKey.setString(KeyFields.USER, user.getName());

            Tuple outputValue = new Tuple();
            outputValue.setInt(ValueFields.DATASET, USERS);
            outputValue.setString(ValueFields.DATA, value.toString());

            context.write(outputKey, outputValue);
        }
    }

    public static class LogMap extends Mapper<LongWritable, Text, Tuple, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            UserLog log = UserLog.fromString(value.toString());
            Tuple outputKey = new Tuple();
            outputKey.setInt(KeyFields.DATASET, USER_LOGS);
            outputKey.setString(KeyFields.USER, log.getName());

            Tuple outputValue = new Tuple();
            outputValue.setInt(ValueFields.DATASET, USER_LOGS);
            outputValue.setString(ValueFields.DATA, value.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static class Reduce extends Reducer<Tuple, Tuple, Text, Text> {
        List<String> users;

        @Override
        protected void reduce(Tuple key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            users = Lists.newArrayList();
            for (Tuple value : values) {
                switch (value.getInt(ValueFields.DATASET)) {
                    case USERS:
                        users.add(value.getString(ValueFields.DATA));
                        break;
                    case USER_LOGS: {
                        String userLog = value.getString(ValueFields.DATA);
                        UserLog log = UserLog.fromString(userLog);
                        for (String user : users) {
                            context.write(new Text(user), new Text(userLog));
                        }
                        break;
                    }
                }
            }
        }
    }
}
