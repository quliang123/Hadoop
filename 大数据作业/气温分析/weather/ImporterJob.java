package cn.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 数据导入作业类
 * 
 * 
 * 数据格式: STN--- WBAN YEARMODA TEMP DEWP SLP STP VISIB WDSP MXSPD GUST MAX MIN
 * PRCP SNDP FRSHTT 545110 99999 20050101 23.9 24 -0.1 24 1024.8 8 1021.2 4 7.7
 * 24 5.9 24 13.6 999.9 35.6* 12.2* 0.00D 999.9 000000
 */

public class ImporterJob {

	public static class DataImportMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		/**
		 * 要求输出类为TableOutputFormat
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if(line.trim().length()==0)
				return;
			if(line.startsWith("STN"))
			 return;//忽略第一行标题头
			String STN = line.substring(0, 6).trim();
			String YEAR = line.substring(14, 18).trim();
			String MODA = line.substring(18, 22).trim();
			String YEARMODA = YEAR + MODA;
			String TEMP = line.substring(24, 28).trim();
			String DEWP = line.substring(35, 41).trim();
			String SLP = line.substring(46, 52).trim();
			String STP = line.substring(57, 63).trim();
			String VISIB = line.substring(68, 73).trim();
			String WDSP = line.substring(78, 83).trim();
			String MXSPD = line.substring(88, 93).trim();
			String GUST = line.substring(95, 100).trim();
			String MAX = line.substring(102, 108).trim();
			String MIN = line.substring(110, 116).trim();
			String PRCP = line.substring(118, 122).trim();
			String SNDP = line.substring(125, 130).trim();
			String FRSHTT = line.substring(132, 138).trim();

			String rowKey = STN + YEARMODA;
			System.out.print(rowKey);
			Put put = new Put(Bytes.toBytes(rowKey));// 按行键添加数据
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STN"),
					Bytes.toBytes(Integer.parseInt(STN)));

			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YEAR"),
					Bytes.toBytes(YEAR));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MODA"),
					Bytes.toBytes(MODA));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YEARMODA"),
					Bytes.toBytes(YEARMODA));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("TEMP"),
					Bytes.toBytes(Double.parseDouble(TEMP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("DEWP"),
					Bytes.toBytes(Double.parseDouble(DEWP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SLP"),
					Bytes.toBytes(Double.parseDouble(SLP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("STP"),
					Bytes.toBytes(Double.parseDouble(STP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("VISIB"),
					Bytes.toBytes(VISIB));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("WDSP"),
					Bytes.toBytes(Double.parseDouble(WDSP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MXSPD"),
					Bytes.toBytes(Double.parseDouble(MXSPD)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("GUST"),
					Bytes.toBytes(Double.parseDouble(GUST)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MAX"),
					Bytes.toBytes(Double.parseDouble(MAX)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MIN"),
					Bytes.toBytes(Double.parseDouble(MIN)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PRCP"),
					Bytes.toBytes(Double.parseDouble(PRCP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SNDP"),
					Bytes.toBytes(Double.parseDouble(SNDP)));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("FRSHTT"),
					Bytes.toBytes(Integer.parseInt(FRSHTT)));

			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)),
					put);
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		
		// 增加HBase中的类型序列化工具类.用于判断是否支持输入的类，根据输入的类给出序列化接口和反序列化接口.
		// io.serializations默认有三个类名：
		//		org.apache.hadoop.io.serializer.WritableSerialization, 
		//		org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization, 
		//		org.apache.hadoop.io.serializer.avro.AvroReflectSerialization
		conf.setStrings("io.serializations", conf.get("io.serializations"),
				MutationSerialization.class.getName(),
				ResultSerialization.class.getName(),
				KeyValueSerialization.class.getName());
		String inputPath = "/input2/weathers_test";
		Path input = new Path(inputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + inputPath);
			return;
		}

		Job job = Job.getInstance(conf, "import data");
		job.setJarByClass(ImporterJob.class);
		job.setMapperClass(DataImportMapper.class);
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
