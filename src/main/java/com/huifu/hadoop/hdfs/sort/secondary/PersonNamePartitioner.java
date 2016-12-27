package com.huifu.hadoop.hdfs.sort.secondary;

import com.huifu.hadoop.hdfs.model.Person;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by chao.hu on 2016/12/27.
 */
public class PersonNamePartitioner extends Partitioner<Person, Text> {

    @Override
    public int getPartition(Person person, Text text, int numPartitions) {
        return Math.abs(person.getLastName().hashCode() * 127) % numPartitions;
    }
}
