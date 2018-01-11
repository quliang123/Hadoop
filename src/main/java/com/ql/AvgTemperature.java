package com.ql;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by 123 on 2018/01/02.
 */
public class AvgTemperature {

    public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
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
            String avgQ = line.substring(36, 42).trim();
            System.out.println("===" + month + "===" + avgQ);
            context.write(new LongWritable(Integer.parseInt(month)), new IntWritable(Integer.valueOf(avgQ)));
        }
    }

    public static class AvgTemperatureReducer extends TableReducer<LongWritable, IntWritable, Text> {

        protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer maxVal = 0; //Integer.MIN_VALUE;      //最小值
            List<Integer> list = new ArrayList<Integer>();
            for (IntWritable writable : values) {   //循环比较
                //  maxVal = Math.min(writable.get(), maxVal);
                maxVal += writable.get();
                list.add(writable.get());
            }

            maxVal /= list.size();

            System.out.println("======" + maxVal);
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("statistic"), Bytes.toBytes("avg"), Bytes.toBytes(maxVal.toString()));
            context.write(new Text(String.valueOf(key)), put);
        }


    }


}
