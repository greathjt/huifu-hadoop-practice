package com.huifu.hadoop.hdfs.join.replicated.framework;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * Created by chao.hu on 2016/12/19.
 */
public class GenericReplicatedJoin extends Mapper<Object, Object, Object, Object> {
    private Path[] distributedFilePahts;
    private File cacheFile;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (paths != null) {
            for (Path path : paths) {
                System.out.println("Distcache file: " + path + " " + path.getName());
                if (path.getName().startsWith("part")) {
                    cacheFile = new File(path.getName());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }

}
