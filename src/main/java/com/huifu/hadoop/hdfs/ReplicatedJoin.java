package com.huifu.hadoop.hdfs;


import com.huifu.hadoop.hdfs.model.User;
import com.huifu.hadoop.hdfs.model.UserLog;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chao.hu on 2016/12/16.
 */
public class ReplicatedJoin extends Configured implements Tool {
    /**
     * Execution: hadoop jar ./huifu-hadoop-practice-1.0.0-SNAPSHOT-shaded.jar
     * com.huifu.hadoop.hdfs.ReplicatedJoin join/user.txt join/logs.txt output
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReplicatedJoin(), args);
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path logsPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Configuration conf = super.getConf();
        //先添加文件，再将conf传给job，否则mapper无法找到文件
        DistributedCache.addCacheFile(usersPath.toUri(), conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReplicatedJoin.class);
        job.setMapperClass(JoinMap.class);
        //否则：job.getConfiguration()，参见上一个注解
        //DistributedCache.addCacheFile(usersPath.toUri(), job.getConfiguration());
        job.getConfiguration().set(JoinMap.DISTCACHE_FILENAME_CONFIG, usersPath.getName());
        FileInputFormat.addInputPath(job, logsPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JoinMap extends Mapper<LongWritable, Text, Text, Text> {
        public static final String DISTCACHE_FILENAME_CONFIG = "replicatedjoin.distcache.filename";
        private Map<String, User> users = new HashMap();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            final String cacheFileName = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);
            boolean found = false;
            if (paths != null) {
                for (Path path : paths) {
                    File file = new File(path.getName());
                    if (cacheFileName.equals(file.getName())) {
                        loadCache(file);
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                throw new IOException("Unable to find file " + cacheFileName);
            }
        }

        private void loadCache(File file) throws IOException {
            for (String line : FileUtils.readLines(file)) {
                User user = User.fromString(line);
                users.put(user.getName(), user);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            UserLog log = UserLog.fromText(value);
            User user = users.get(log.getName());
            if (user != null) {
                context.write(new Text(user.toString()), new Text(log.toString()));
            }
        }
    }
}
