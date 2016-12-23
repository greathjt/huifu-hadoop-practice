package com.huifu.hadoop.hdfs.join.bloom;

import com.huifu.hadoop.hdfs.model.User;
import com.huifu.hadoop.util.AvroBytesRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;

/**
 * Created by chao.hu on 2016/12/23.
 */
public class BloomFilterCreator extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BloomFilterCreator(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path userPath = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(BloomFilterCreator.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(FilterReducer.class);

        AvroJob.setOutputKeySchema(job, AvroBytesRecord.SCHEMA);
        job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, SnappyCodec.class.getName());

        output.getFileSystem(job.getConfiguration()).delete(output, true);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        FileInputFormat.setInputPaths(job, userPath);
        FileOutputFormat.setOutputPath(job, output);

        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, NullWritable, BloomFilter> {
        private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            User user = User.fromText(value);
            if ("CA".equalsIgnoreCase(user.getState())) {
                filter.add(new Key(user.getName().getBytes()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), filter);
        }

    }

    public static class FilterReducer extends Reducer<NullWritable, BloomFilter, AvroKey<GenericRecord>, NullWritable> {
        private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);

        @Override
        protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            for (BloomFilter value : values) {
                filter.and(value);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new AvroKey<GenericRecord>(AvroBytesRecord.toGenericRecord(filter)), NullWritable.get());
        }
    }
}