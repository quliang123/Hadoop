package com.com;

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
import java.util.StringTokenizer;

/**
 * Created by 123 on 2017/12/31.
 *
 * @author 123
 */
public class importData2 {

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
            String rowkey="Ql-"+stn+"-ql";
            Put put = new Put(Bytes.toBytes(rowkey));
            StringTokenizer tokenizer = new StringTokenizer(stn,"\n");
            while (tokenizer.hasMoreTokens()){
                StringTokenizer token = new StringTokenizer(tokenizer.nextToken(),"\t");
                String token_value=token.nextToken();
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STN"),
                        Bytes.toBytes(token_value));
            }

//            Iterable<Text> value3=(Iterable<Text>)value;
//            for(Text v:value3){
//                String[] splited = v.toString().split(",");
//                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STN"),
//                        Bytes.toBytes(splited[0]));
//            }
//            if (line.contains("区站号")) return;

        //    System.out.println(line + "================================");
            //准备数据
            //54511

           /* String year = line.substring(24, 28);
            String month = line.substring(30, 32);
            String day = line.substring(32, 34);*/

          /*  String yearMoth = stn + year + month + day;*/

       /*   System.out.println(yearMoth + "==============" + line.length());*/

          /*  String avgQ = line.substring(38, 41);*/

           /*String maxQ = line.substring(45, 48);

            String minQ = line.substring(52, 55);*/



           /*  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YEAR"),
                    Bytes.toBytes(Integer.parseInt(year)));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MOTH"),
                    Bytes.toBytes(Integer.parseInt(month)));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DAY"),
                    Bytes.toBytes(Integer.parseInt(day)));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("avgQ"),
                    Bytes.toBytes(Integer.parseInt(avgQ)));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("maxQ"),
                    Bytes.toBytes(Integer.parseInt(maxQ)));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("minQ"),
                    Bytes.toBytes(Integer.parseInt(minQ)));*/

            context.write(new ImmutableBytesWritable(Bytes.toBytes(stn)), put);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();

        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(),
                ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());

        String inputPath = "/input11/";
        Path input = new Path(inputPath);
        if (!FileSystem.get(conf).exists(input)) {
            System.out.println("路径不存在:" + inputPath);
            return;
        }

        MyMapper mapper = new MyMapper();


        Job job = Job.getInstance(conf, "import data");
        job.setJarByClass(importData2.class);
        job.setMapperClass(MyMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "weathers");
        FileInputFormat.addInputPath(job, input);
        // 为任务添加HBase依赖库
        TableMapReduceUtil.addDependencyJars(job);

        job.waitForCompletion(true);// 提交作业

        System.out.println(mapper.line + "=============" + mapper);
    }
}
