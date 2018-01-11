package com.wgy.bigdata.hbase.zjk;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Created by Wgy on 2017年12月21日
 */
public class ImportDataToHbase implements Serializable {
	private static final long serialVersionUID = 449373722017672916L;
	public static class BatchMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(",");
			if (split.length <= 0) {
				return;
			}
			String time = split[2];
			if (time == null || time.equals("")) {
				return;
			}
			long l = TimeUtils.parseString2Long(time, "yyyy-MM-dd HH:mm:ss");
			// 测点编号-年月日(YYYYMMDD)结合，使用-符号分割 303882-20171221
			Text k = new Text(split[5] + "-" + TimeUtils.parseLong2String(l * 1000, "yyyyMMdd"));
			context.write(k, value);
		}
	}
	static class BatchReducer extends TableReducer<Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {
			setup(context);
			Configuration conf = context.getConfiguration();
			String lxbm = conf.get("lx");
			String rowKey = key.toString();
			StringBuffer buffer = new StringBuffer();
			String pointNo = rowKey.substring(0, 6);
			String time = rowKey.substring(6, 14);
			// 测点编号+练习编码+年月日(YYYYMMDD)结合，使用-符号分割，如300012-LX-LX01-20171221
			String rowKey2 = pointNo + buffer.append("QL").append("-").append(lxbm).append("-").append(time);
			Put put = new Put(Bytes.toBytes(rowKey2));
			for (Text v : iterable) {
				String[] split= v.toString().split(",");
				// 将指定格式的时间字符串转换为时间戳
				Long l = TimeUtils.parseString2Long(rowKey.split("-")[1], "yyyyMMdd");
				Long l2 = TimeUtils.parseString2Long(split[2], "yyyy-MM-dd HH:mm:ss");
				String l3 = TimeUtils.getCompleteString(String.valueOf(l2 - l));
				put.addColumn(Bytes.toBytes("d"), Bytes.toBytes(l3), Bytes.toBytes(split[1]));
			}
			context.write(NullWritable.get(), put);
		}
	}

	// reduce要输出的表名
	private static String tableName = "HB_QL_TS";

	/**
	 * 描述并提交job
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// 实例化配置类
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://XXXX:8020/hbase");
		// zookkeeper 地址 集群服务器
		conf.set("hbase.zookeeper.quorum", "XXX,XXX,XXX");
		conf.set("lx", args[1]);
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		Job job = new Job(conf, ImportDataToHbase.class.getSimpleName());
		TableMapReduceUtil.addDependencyJars(job);
		job.setJarByClass(ImportDataToHbase.class);
		job.setMapperClass(BatchMapper.class);
		job.setReducerClass(BatchReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		// 如果不设置InputFormat，它默认用的是TextInputformat.class
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.setInputPaths(job, args[0]);
		job.waitForCompletion(true);
	}

}
