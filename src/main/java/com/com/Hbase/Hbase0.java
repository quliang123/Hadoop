package com.com.Hbase;

import java.io.IOException;
import java.util.Iterator;

import com.sun.javafx.geom.Vec4d;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * hbase
 * 迭代所有数据表
 * Hello world!
 */
public class Hbase0 {
    static Configuration cfg = new Configuration();

    public static void main(String[] args) throws IOException {
        getData();
    }

    public static void getData() throws IOException {
        Connection con = ConnectionFactory.createConnection(cfg);
        Admin admin = con.getAdmin();
        System.out.println("list table");
        for (TableName name : admin.listTableNames()) {
            System.out.println(name);
        }
        con.close();
    }

}
