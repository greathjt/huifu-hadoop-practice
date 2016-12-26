package com.huifu.hadoop.hdfs.join.bloom;

import com.google.common.collect.Lists;

import com.huifu.hadoop.hdfs.model.User;
import com.huifu.hadoop.hdfs.model.UserLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.htuple.Tuple;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by chao.hu on 2016/12/26.
 */
public class BloomJoin extends Configured implements Tool {
    enum ValueFields {
        DATASET,
        DATA
    }

    public static final int USERS = 0;
    public static final int USER_LOGS = 1;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BloomJoin(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path userLogsPath = new Path(args[1]);
        Path filterPath = new Path(args[2]);
        Path output = new Path(args[3]);

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(BloomJoin.class);

        DistributedCache.addCacheFile(filterPath.toUri(), job.getConfiguration());
        job.getConfiguration().set(AbstractFilterMap.DISTCACHE_FILENAME_CONFIG, filterPath.getName());

        MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
        MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);

        job.setReducerClass(BloomReducer.class);
        FileOutputFormat.setOutputPath(job, output);
        FileInputFormat.setInputPaths(job, usersPath);
        FileInputFormat.setInputPaths(job, userLogsPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class BloomReducer extends Reducer<Text, Tuple, Text, Text> {
        List<String> users;
        List<String> userLogs;

        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            users = Lists.newArrayList();
            userLogs = Lists.newArrayList();

            for (Tuple tuple : values) {
                System.out.println("Tuple: " + tuple);
                switch (tuple.getInt(ValueFields.DATASET)) {
                    case USERS: {
                        users.add(tuple.getString(ValueFields.DATA));
                        break;
                    }
                    case USER_LOGS: {
                        userLogs.add(tuple.getString(ValueFields.DATA));
                        break;
                    }
                }
            }

            for (String user : users) {
                System.out.println("#############" + user);
                for (String userLog : userLogs) {
                    context.write(new Text(user), new Text(userLog));
                }
            }

        }
    }

    public static abstract class AbstractFilterMap extends Mapper<LongWritable, Text, Text, Tuple> {
        public static final String DISTCACHE_FILENAME_CONFIG = "bloomjoin.distcache.filename";
        private BloomFilter filter;

        abstract String getUserName(Text value);

        abstract int getDataSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            final String filterFileName = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);
            filter = BloomFilterCreator.fromFile(new File(paths[0].getName()));
            System.out.println(paths[0].getName() + "???????????????");
            System.out.println(">>>>>>>>>>>><<<<<<<<<<<<<<<<<" + filter.toString());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String userName = getUserName(value);
            if (filter.membershipTest(new Key(userName.getBytes()))) {
                System.out.println(userName + ">>>>>>>>>>>>>>>>>>>>>>>>>>>");
                Tuple tuple = new Tuple();
                tuple.setInt(ValueFields.DATASET, getDataSet());
                tuple.setString(ValueFields.DATA, value.toString());
                context.write(new Text(userName), tuple);
            }
        }
    }

    public static class UserMap extends AbstractFilterMap {

        @Override
        String getUserName(Text value) {
            User user = User.fromText(value);
            return user.getName();
        }

        @Override
        int getDataSet() {
            return USERS;
        }
    }

    public static class UserLogMap extends AbstractFilterMap {
        @Override
        String getUserName(Text value) {
            UserLog log = UserLog.fromText(value);
            return log.getName();
        }

        @Override
        int getDataSet() {
            return USER_LOGS;
        }
    }
}
