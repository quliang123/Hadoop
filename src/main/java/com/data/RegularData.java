package com.data;

import com.ql.MinTemperature;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 123 on 2018/01/02.
 */
public class RegularData {


    /**
     * LongWritableText         偏移量
     * Text                      行文本
     * Text                      reduce   的key
     * DoubleWritable            值
     */
    public static class dataTemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        /**
         * @param key     偏移量  offset
         * @param value   单行文本
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();

            line = line.replaceAll("\"", "");

            String[] arr = line.trim().split(",");


            int j = 1;   //值
            int k = 2;   //时间
            int l = 5;  //编码

            for (int i = 0; i < arr.length; i++) {
                // System.out.println(item);
                /*if (j >= arr.length) {
                    return;
                }*/
                if (arr[i].contains("id")) {
                    return;
                }


                System.out.println(j + "========" + k + "=======" + l);
                //if (j != 1 && k != 2 && l != 5) {
                // System.out.println(j + "==============" + arr[j]);
                String row = arr[l] + "-" + "QL" + "-" + arr[k];
                System.out.println(row + "====" + arr[j]);
                context.write(new Text(row), new DoubleWritable(Double.valueOf(arr[j])));
                // }
               /* j += 6;
                k += 6;
                l += 6;*/


            }
        }
    }

    public static class dataTemperatureReducer extends TableReducer<Text, DoubleWritable, Text> {
        /**
         * @param key     key  不多说     一般都是hbase中的rowkey
         * @param values  迭代器    如果同名key有其他的值得话列如     [1,2,3,4,45,66],     没有则是   1
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            if (values.iterator().hasNext()) {
                Put put = new Put(Bytes.toBytes(key.toString()));   //rowkey
                put.addColumn(Bytes.toBytes("val"), Bytes.toBytes("val"), Bytes.toBytes(values.iterator().next().toString()));
                context.write(key, put);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration config = HBaseConfiguration.create();

        Path input = new Path("hdfs://master:9000/input12/");
        if (!FileSystem.get(config).exists(input)) {
            System.out.println("路径不存在:" + "");
            return;
        }
        // Configuration conf = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");


        Job job = Job.getInstance(config, "max temperature");
        try {

            job.setJarByClass(RegularData.class);
            job.setMapperClass(RegularData.dataTemperatureMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setOutputKeyClass(Text.class);
            //job.setOutputFormatClass(TableOutputFormat.class);
            //job.setOutputFormatClass(MultiTableOutputFormat.class);

            job.setOutputValueClass(Mutation.class);
        } catch (Exception ex) {
            System.err.println("错误" + ex.getMessage());
        }

        FileInputFormat.addInputPath(job, input);
        TableMapReduceUtil.initTableReducerJob("csvData", RegularData.dataTemperatureReducer.class, job);

        boolean sum = job.waitForCompletion(true);
        System.out.println("=================" + sum);
    }
}

