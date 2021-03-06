package com.huifu.hadoop.hdfs.model;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

/**
 * Created by chao.hu on 2016/12/16.
 */
public class User {
    private final String name;
    private final int age;
    private final String state;


    public User(String name, int age, String state) {
        this.name = name;
        this.age = age;
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getState() {
        return state;
    }

    public static User fromText(Text text) {
        return fromString(text.toString());
    }

    public static User fromString(String str) {
        String[] parts = StringUtils.split(str);
        return new User(parts[0], Integer.valueOf(parts[1]), parts[2]);
    }

    @Override
    public String toString() {
        return String.format("%s\t%d\t%s", name, age, state);
    }
}
