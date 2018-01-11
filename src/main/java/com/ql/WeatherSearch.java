package com.ql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 * Created by 123 on 2018/01/02.
 */

public class WeatherSearch {
    //配置
    private static Configuration config = HBaseConfiguration.create();
    //表名
    private static final String WeatherTableName = "weathers";
    //列族
    private static final String WeatherTableFamily = "info";
    // 数据文件的存储地址
    private static final String defaultPath = "hdfs://master:9000/input13/";

    private static final String ForecastTableName = "forecast";
    //汇总结果表
    private static String resultTableName = "results";

    static Scanner input = new Scanner(System.in);


    public static void main(String[] args) throws Exception {
        // System.out.println("========="+args[0]);
        // System.setProperty("hadoop.home.dir", "D:\\BigData\\bigData\\hadoop-2.8.0");
        int op = 0;
        do {
            System.out.println("天气查询系统:");
            System.out.println("1.查询某一天的天气数据");
            System.out.println("2.查询本月的最高气温");
            System.out.println("3.查询本月的最低气温");
            System.out.println("4.查询本月的平均气温");
            System.out.println("5.预测明天的气温");
            System.out.println("0.退出");
            System.out.print("请选择[0-6]进行操作:");
            op = input.nextInt();
            switch (op) {
                case 1:
                    getWeather();  //  某一天的数据
                    break;
                case 2:
                    statisticMaxTemp();  //一个月中的最高气温
                    break;
                case 3:
                    statisticMinTemp();  //一个月中的最低气温
                    break;
                case 4:
                    statisticAvgTemp(); //一个月中的平均气温
                    break;
                case 5:
                    forecastWeather();  //预测明天天气
                    break;
                case 0:
                    break;
                default:
                    break;
            }

        } while (op > 0);


    }

    private static void forecastWeather() throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(defaultPath);
        if (!FileSystem.get(config).exists(input)) {
            System.out.println("路径不存在:" + defaultPath);
            return;
        }
        // Configuration conf = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");


        Job job = Job.getInstance(config, "Speculation temperature");
        try {
            job.setJarByClass(SpeculationTemperature.class);
            job.setMapperClass(SpeculationTemperature.SpeculationTemperatureMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MapWritable.class);

            job.setOutputKeyClass(Text.class);
            //job.getConfiguration().set("date", new SimpleDateFormat("yyyyMMdd").format(new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000)));


            //job.setOutputFormatClass(TableOutputFormat.class);
            //job.setOutputFormatClass(MultiTableOutputFormat.class);

            job.setOutputValueClass(Mutation.class);
        } catch (Exception ex) {
            System.err.println("错误" + ex.getMessage());
        }

        FileInputFormat.addInputPath(job, input);
        TableMapReduceUtil.initTableReducerJob(ForecastTableName, SpeculationTemperature.SpeculationTemperatureReducer.class, job);

        if (job.waitForCompletion(true)) {
            //  printInfo("avg", "statistic");
        }


    }


    private static void statisticAvgTemp() throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(defaultPath);
        if (!FileSystem.get(config).exists(input)) {
            System.out.println("路径不存在:" + defaultPath);
            return;
        }
        // Configuration conf = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");


        Job job = Job.getInstance(config, "avg temperature");
        try {

            job.setJarByClass(AvgTemperature.class);
            job.setMapperClass(AvgTemperature.AvgTemperatureMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            //job.setOutputFormatClass(TableOutputFormat.class);
            //job.setOutputFormatClass(MultiTableOutputFormat.class);

            job.setOutputValueClass(Mutation.class);
        } catch (Exception ex) {
            System.err.println("错误" + ex.getMessage());
        }

        FileInputFormat.addInputPath(job, input);
        TableMapReduceUtil.initTableReducerJob(resultTableName, AvgTemperature.AvgTemperatureReducer.class, job);

        if (job.waitForCompletion(true)) {
            printInfo("avg", "statistic");
        }

    }


    /**
     * 最低气温
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private static void statisticMinTemp() throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(defaultPath);
        if (!FileSystem.get(config).exists(input)) {
            System.out.println("路径不存在:" + defaultPath);
            return;
        }
        // Configuration conf = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");


        Job job = Job.getInstance(config, "max temperature");
        try {

            job.setJarByClass(MinTemperature.class);
            job.setMapperClass(MinTemperature.MinTemperatureMapper.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            //job.setOutputFormatClass(TableOutputFormat.class);
            //job.setOutputFormatClass(MultiTableOutputFormat.class);


            job.setOutputValueClass(Mutation.class);
        } catch (Exception ex) {
            System.err.println("错误" + ex.getMessage());
        }

        FileInputFormat.addInputPath(job, input);
        TableMapReduceUtil.initTableReducerJob(resultTableName, MinTemperature.MinTemperatureReducer.class, job);

        if (job.waitForCompletion(true)) {
            printInfo("min", "statistic");
        }

    }


    /**
     * 最高气温
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private static void statisticMaxTemp() throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path(defaultPath);
        if (!FileSystem.get(config).exists(input)) {
            System.out.println("路径不存在:" + defaultPath);
            return;
        }
        // Configuration conf = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");


        Job job = Job.getInstance(config, "max temperature");
        try {

            job.setJarByClass(MaxTemperature.class);
            job.setMapperClass(MaxTemperature.MaxTemperatureMapper.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            //job.setOutputFormatClass(TableOutputFormat.class);
            //job.setOutputFormatClass(MultiTableOutputFormat.class);


            job.setOutputValueClass(Mutation.class);
        } catch (Exception ex) {
            System.err.println("错误" + ex.getMessage());
        }

        FileInputFormat.addInputPath(job, input);
        TableMapReduceUtil.initTableReducerJob(resultTableName, MaxTemperature.MaxTemperatureReducer.class, job);

        if (job.waitForCompletion(true)) {
            printInfo("max", "statistic");
        }
    }


    /**
     * 获取某一天的气温
     *
     * @throws IOException
     */
    public static void getWeather() throws IOException {
        System.out.println("请输入日期:(yyyyMMdd)");
        String date = input.next();

        System.out.println("请输入站点()");
        String site = input.next();

        Connection con = ConnectionFactory.createConnection(config);

        Table table = con.getTable(TableName.valueOf(WeatherTableName));

        Scan scan = new Scan();

        scan.addFamily(Bytes.toBytes("info"));

        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("DAY"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(date)));

        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(site))));

        ResultScanner rs = table.getScanner(scan);
        for (Result row : rs) {
            byte[] stn = row.getValue(Bytes.toBytes("info"), Bytes.toBytes("STN"));

            byte[] yearmoda = row.getValue(Bytes.toBytes("info"), Bytes.toBytes("YEARMODAY"));

            byte[] avgq = row.getValue(Bytes.toBytes("info"), Bytes.toBytes("AVGQ"));

            byte[] maxq = row.getValue(Bytes.toBytes("info"), Bytes.toBytes("MAXQ"));

            byte[] minq = row.getValue(Bytes.toBytes("info"), Bytes.toBytes("MINQ"));

            System.out.println("站点" + Bytes.toString(stn) + "\t日期:" + Bytes.toString(yearmoda) + "\t平均气温:" + Bytes.toString(avgq) + "\t最高气温:" + Bytes.toString(maxq) + "\t最低气温:" + Bytes.toString(minq));
        }
        con.close();
    }


    public static void printInfo(String type, String family) throws IOException {
        Connection con = ConnectionFactory.createConnection();
        Table table = con.getTable(TableName.valueOf(resultTableName));
        ResultScanner rs = table.getScanner(Bytes.toBytes(family), Bytes.toBytes(type));
        for (Result result : rs) {
            if ("max".equals(type)) {
                System.out.println("月份:" + "\t" +
                        Bytes.toString(result.getRow()) + "\t" +
                        Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(type)
                                )
                        )
                );
            } else if ("min".equals(type)) {
                System.out.println("月份:" + "\t" +
                        Bytes.toString(result.getRow()) + "\t" +
                        Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(type)
                                )
                        )
                );
            }
        }
        con.close();
    }

}
