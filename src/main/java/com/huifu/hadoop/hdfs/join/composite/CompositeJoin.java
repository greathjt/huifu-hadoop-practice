package com.huifu.hadoop.hdfs.join.composite;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by chao.hu on 2016/12/19.
 */
public class CompositeJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CompositeJoin(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path userPath = new Path(args[0]);
        Path logPath = new Path(args[1]);
        Path output = new Path(args[2]);
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CompositeJoin.class);
        job.setMapperClass(JoinMap.class);
        job.setInputFormatClass(CompositeInputFormat.class);
        job.getConfiguration().set(CompositeInputFormat.JOIN_EXPR, CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, userPath, logPath));
//        job.getConfiguration().set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"\t");
        job.setNumReduceTasks(0);
        output.getFileSystem(job.getConfiguration()).delete(output, true);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static class JoinMap extends Mapper<Text, TupleWritable, Text, Text> {
        @Override
        protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
            String[] values = {
                    value.get(0).toString(), value.get(1).toString()
            };
//            System.out.println("######"+ value.get(0).toString());
            context.write(key, new Text(StringUtils.join(values, "\t")));
        }
    }
}
