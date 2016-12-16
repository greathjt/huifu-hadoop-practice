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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chao.hu on 2016/12/16.
 */
public class ReplicatedJoin extends Configured implements Tool {

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
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReplicatedJoin.class);
        DistributedCache.addCacheFile(usersPath.toUri(), conf);
        //config.set("mapred.cache.files", usersPath.toUri().toString());
        //System.out.println("<<<<<<<<<<<<<<<<<<<<<"+usersPath.toUri().toString());
        job.setMapperClass(JoinMap.class);
        job.getConfiguration().set(JoinMap.DISTCACHE_FILENAME_CONFIG, usersPath.getName());
        FileInputFormat.addInputPath(job, logsPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JoinMap extends Mapper<LongWritable, Text, Text, Text> {
        public static final String DISTCACHE_FILENAME_CONFIG = "replicatedjoin.distcache.filename";
        private Map<String, User> users = new HashMap<String, User>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] uris = DistributedCache.getCacheArchives(context.getConfiguration());
            final String cacheFileName = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);
            boolean found = false;
            if (uris != null) {
                for (URI uri : uris) {
                    File file = new File(uri.getPath());
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
