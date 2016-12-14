package com.huifu.hadoop.hbase;


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExportedReader extends Configured implements Tool {
    /**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        int res = ToolRunner.run(new Configuration(), new ExportedReader(), args);
        System.exit(res);
    }

    /**
     * Read sequence file from HDFS created by HBase export.
     *
     * @param args the command-line arguments
     * @return the process exit code
     * @throws Exception if something goes wrong
     */
    public int run(final String[] args) throws Exception {

        Path inputFile = new Path("/user/root/bd_page/part-m-00000");

        Configuration conf = super.getConf();
        conf.setStrings("io.serializations", conf.get("io.serializations"),
                ResultSerialization.class.getName());

        SequenceFile.Reader reader =
                new SequenceFile.Reader(conf, SequenceFile.Reader.file(inputFile));

        try {
            ImmutableBytesWritable key = new ImmutableBytesWritable();
            Result value = new Result();

            while (reader.next(key)) {
                value = (Result) reader.getCurrentValue(value);
                System.out.println(new String(key.get()) + ":" + ToStringBuilder.reflectionToString(value, ToStringStyle.SIMPLE_STYLE));
            }
        } finally {
            reader.close();
        }
        return 0;
    }
}
