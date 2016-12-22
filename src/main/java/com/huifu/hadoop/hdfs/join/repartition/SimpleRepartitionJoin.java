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
import org.htuple.Tuple;

import java.io.IOException;
import java.util.List;

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

        MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
        MultipleInputs.addInputPath(job, logsPath, TextInputFormat.class, LogMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setReducerClass(Reduce.class);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    enum ValueFields {
        DATASET,
        DATA
    }

    private static final int USERS = 0;
    private static final int USER_LOGS = 1;

    public static class UserMap extends Mapper<LongWritable, Text, Text, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("###########"+key.toString());
            User user = User.fromString(value.toString());
            Tuple tuple = new Tuple();
            tuple.setInt(ValueFields.DATASET, USERS);
            tuple.setString(ValueFields.DATA, value.toString());
            context.write(new Text(user.getName()), tuple);
        }
    }

    public static class LogMap extends Mapper<LongWritable, Text, Text, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            UserLog log = UserLog.fromString(value.toString());
            Tuple tuple = new Tuple();
            tuple.setInt(ValueFields.DATASET, USER_LOGS);
            tuple.setString(ValueFields.DATA, value.toString());
            context.write(new Text(log.getName()), tuple);
        }
    }

    public static class Reduce extends Reducer<Text, Tuple, Text, Text> {
        private List<String> users;
        private List<String> logs;

        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            users = Lists.newArrayList();
            logs = Lists.newArrayList();
            for (Tuple tuple : values) {
                switch (tuple.getInt(ValueFields.DATASET)) {
                    case USERS: {
                        users.add(tuple.getString(ValueFields.DATA));
                        break;
                    }
                    case USER_LOGS: {
                        logs.add(tuple.getString(ValueFields.DATA));
                        break;
                    }
                }
            }
            //cartesian
            for (String user : users) {
                for (String log : logs) {
                    context.write(new Text(user), new Text(log));
                }
            }
        }
    }
}
