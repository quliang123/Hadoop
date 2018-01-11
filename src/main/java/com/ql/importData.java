package com.ql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by 123 on 2017/12/31.
 *从HDFS系统上把数据导入HBase
 *  导入数据
 *
 * @author 123
 */
public class importData {

    static class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        String line = null;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            line = value.toString();
            System.out.println(line.length() + "=============" + line);
            if (line.length() == 0) return;
            //stn+year+month+day
//54511 3948 116281111313 2017  1  1    -40    -15    -58 0 0 0
            String stn = line.substring(0, 5).trim();//54511

            String year = line.substring(24, 28).trim();//2017   24  28
            String month = line.substring(30, 32).trim();//1
            if (Integer.parseInt(month) < 10) {
                month = "0" + month;
            }


            String day = line.substring(32, 35).trim();   //1
            if (Integer.parseInt(day) < 10) {
                day = "0" + day;
            }
            String yearMoDay = year + month + day;

            String rowkey = stn + year + month + day;


            System.out.println(rowkey + "==============" + line.length());
            ;
            String avgQ = line.substring(36, 42).trim(); //.trim();
            String maxQ = line.substring(43, 49).trim();  //.trim();
            String minQ = line.substring(50, 56).trim();


            Put put = new Put(Bytes.toBytes(rowkey));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STN"),
                    Bytes.toBytes(stn.toString()));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YEAR"),
                    Bytes.toBytes(year.toString()));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MOTH"),
                    Bytes.toBytes(month.toString()));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DAY"),
                    Bytes.toBytes(day.toString()));


            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YEARMODAY"), Bytes.toBytes(yearMoDay));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("AVGQ"),
                    Bytes.toBytes(avgQ.toString()));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MAXQ"),
                    Bytes.toBytes(maxQ.toString()));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MINQ"),
                    Bytes.toBytes(minQ.toString()));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey.toString())), put);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();

        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(),
                ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());

        String inputPath = "/input13/";            //          /input11/
        Path input = new Path(inputPath);
        if (!FileSystem.get(conf).exists(input)) {
            System.out.println("路径不存在:" + inputPath);
            return;
        }


        Job job = Job.getInstance(conf, "import data");
        job.setJarByClass(importData.class);
        job.setMapperClass(MyMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "weathers");
        FileInputFormat.addInputPath(job, input);
        // 为任务添加HBase依赖库
        TableMapReduceUtil.addDependencyJars(job);

        job.waitForCompletion(true);// 提交作业

    }
}
