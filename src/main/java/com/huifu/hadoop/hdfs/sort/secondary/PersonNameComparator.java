package com.huifu.hadoop.hdfs.sort.secondary;

import com.huifu.hadoop.hdfs.model.Person;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by chao.hu on 2016/12/27.
 */
public class PersonNameComparator extends WritableComparator {
    protected PersonNameComparator() {
        super(Person.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        Person p1 = (Person) o1;
        Person p2 = (Person) o2;
        return p1.getLastName().compareTo(p2.getLastName());
    }
}
