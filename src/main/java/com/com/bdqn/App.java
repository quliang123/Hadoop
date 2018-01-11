package com.com.bdqn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {

        int a = 1;
        Configuration config = new Configuration();

        FileSystem fs = FileSystem.get(config);
        fs.mkdirs(new Path("fuck"));
        fs.close();
    }
}
