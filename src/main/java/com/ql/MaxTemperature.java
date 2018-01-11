package com.ql;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 123 on 2018/01/02.
 * 本月最高气温
 */
public class MaxTemperature {

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //String year = line.substring(24, 28).trim();
            String month = line.substring(30, 32).trim();
            if (Integer.parseInt(month) < 10) {
                month = "0" + month;
            }
            String day = line.substring(32, 35).trim();   //1
            if (Integer.parseInt(day) < 10) {
                day = "0" + day;
            }
            String maxQ = line.substring(44, 49).trim();
            System.out.println("===" + day + "===" + maxQ);
            context.write(new LongWritable(Integer.parseInt(month)), new IntWritable(Integer.valueOf(maxQ)));
        }
    }

    public static class MaxTemperatureReducer extends TableReducer<LongWritable, IntWritable, Text> {

        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer minVal = Integer.MIN_VALUE; //Integer.MIN_VALUE;      //最小值
            Integer max = 0;
            for (IntWritable writable : values) {   //循环比较
                // minVal =            //拿出迭代器中的一项值
                System.out.println("===" + minVal);
                minVal = Math.max(writable.get(), minVal);              //拿到最大值   100      ???    拿到左边的值        以此类推,迭代完成,拿到最大值
            }
            System.out.println("======" + minVal);
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("statistic"), Bytes.toBytes("max"), Bytes.toBytes(minVal.toString()));
            context.write(new Text(String.valueOf(key)), put);
        }


    }


}
