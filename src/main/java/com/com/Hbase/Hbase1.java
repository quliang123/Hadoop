package com.com.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author 123
 * Hbase
 * 创建表
 * Hello world!
 */
public class Hbase1 {
    static Configuration cfg = new Configuration();

    public static void main(String[] args) throws IOException {
        createTable();
    }

    public static void createTable() throws IOException {
        Connection con = ConnectionFactory.createConnection(cfg);
        Admin admin = con.getAdmin();
        System.out.println("list table");

        TableName tName = TableName.valueOf("HbaseTest");
        HTableDescriptor table = new HTableDescriptor(tName);

        HColumnDescriptor column1 = new HColumnDescriptor("name");
        HColumnDescriptor column2 = new HColumnDescriptor("score");

        table.addFamily(column1);
        table.addFamily(column2);

        admin.createTable(table);

        System.out.println("创建数据表成功");
        con.close();
    }

}
