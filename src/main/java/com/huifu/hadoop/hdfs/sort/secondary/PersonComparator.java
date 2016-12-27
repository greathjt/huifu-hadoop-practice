package com.huifu.hadoop.hdfs.sort.secondary;

import com.huifu.hadoop.hdfs.model.Person;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by chao.hu on 2016/12/27.
 */
public class PersonComparator extends WritableComparator {
    protected PersonComparator() {
        super(Person.class, true);
    }
    @Override
    public int compare(Object w1, Object w2) {
        Person p1 = (Person) w1;
        Person p2 = (Person) w2;

        int cmp = p1.getLastName().compareTo(p2.getLastName());
        if (cmp != 0) {
            return cmp;
        }

        return p1.getFirstName().compareTo(p2.getFirstName());
    }
}
