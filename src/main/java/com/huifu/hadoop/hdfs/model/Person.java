package com.huifu.hadoop.hdfs.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by chao.hu on 2016/12/27.
 */
public class Person implements WritableComparable<Person> {
    private String firstName;
    private String lastName;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(firstName);
        out.writeUTF(lastName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.firstName = in.readUTF();
        this.lastName = in.readUTF();
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public int compareTo(Person p2) {
        int cmp = this.getLastName().compareTo(p2.getLastName());
        if (cmp != 0) {
            return cmp;
        }
        return this.getFirstName().compareTo(p2.getFirstName());
    }
}
