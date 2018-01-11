package com.ql;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by 123 on 2018/01/02.
 * 利用以往数据,预测明天气温
 */
public class SpeculationTemperature {

    public static class SpeculationTemperatureMapper extends Mapper<LongWritable, Text, LongWritable, MapWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String stn = line.substring(0, 5).trim();

            String avgQ = line.substring(36, 42).trim(); //.trim();
            String maxQ = line.substring(43, 49).trim();  //.trim();
            String minQ = line.substring(50, 56).trim();

            String year = line.substring(24, 28).trim();//2017   24  28

            String month = line.substring(30, 32).trim();
            if (Integer.parseInt(month) < 10) {
                month = "0" + month;
            }

            String day = line.substring(32, 35).trim();   //1
            if (Integer.parseInt(day) < 10) {
                day = "0" + day;
            }

            String yearMonthDay = year + month + day;

            MapWritable map = new MapWritable();
            map.put(new Text("yearMonthDay"), new Text(yearMonthDay));
            map.put(new Text("stn"), new Text(stn));
            map.put(new Text("maxQ"), new IntWritable(Integer.parseInt(maxQ)));
            map.put(new Text("minQ"), new IntWritable(Integer.parseInt(minQ)));
            map.put(new Text("avgQ"), new IntWritable(Integer.parseInt(avgQ)));


            context.write(new LongWritable(Integer.parseInt(stn + month)), map);
        }
    }

    public static class SpeculationTemperatureReducer extends TableReducer<LongWritable, MapWritable, Text> {

        protected void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            Integer maxQ = 0;
            Integer avgQ = 0;
            Integer minQ = 0;

            int sum = 0;

            for (MapWritable writable : values) {   //循环比较
                maxQ += ((IntWritable) writable.get(new Text("maxQ"))).get();
                avgQ += ((IntWritable) writable.get(new Text("avgQ"))).get();
                minQ += ((IntWritable) writable.get(new Text("minQ"))).get();
                sum++;
            }


            maxQ = maxQ / sum;
            avgQ = avgQ / sum;
            minQ = minQ / sum;
            //   System.out.println("======" + minVal);

          //  String date = context.getConfiguration().get("date");

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("max"), Bytes.toBytes(maxQ.toString()));
            put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("avg"), Bytes.toBytes(avgQ.toString()));
            put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("min"), Bytes.toBytes(minQ.toString()));
            put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("date"), Bytes.toBytes(new SimpleDateFormat("yyyyMMdd").format(new Date())));

            context.write(new Text(String.valueOf(key)), put);
        }


    }


}
