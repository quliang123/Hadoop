package cn.weather;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cn.weather.ImporterJob.DataImportMapper;

public class JobDriver {

	// 配置
	private static Configuration conf = HBaseConfiguration.create();
	// 表名和列族
	private static final String WEATHERS = "weathers";
	private static final String WEATHERS_FAMILY = "info";// 如有多个列族以逗号分隔
	private static final String RESULTS = "results";
	private static final String RESULTS_FAMILY = "statistic";
	private static final String FORECAST = "forecast";
	private static final String FORECAST_FAMILY = "item";
	/**
	 * 默认气象数据所在HDFS目录
	 */
	private static String defaultInputPath = "/input2/weathers";

	/**
	 * 程序入口
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		int op = 0;
		// 初始化数据库
		init();
		do {
			System.out.println("天气查询系统:");
			System.out.println("1.查询某一天的天气数据");
			System.out.println("2.查询每年的最高气温");
			System.out.println("3.查询每年的最低气温");
			System.out.println("4.查询每年的平均气温");
			System.out.println("5.查询每年的下雨的天数");
			System.out.println("6.预测明天的气温");
			System.out.println("0.退出");
			System.out.print("请选择[0-6]进行操作:");
			Scanner scan = new Scanner(System.in);
			op = scan.nextInt();

			switch (op) {

			case 1:
				getWeather();
				break;
			case 2:
				statisticMaxTemp();
				break;
			case 3:
				statisticMinTemp();
				break;
			case 4:
				statisticAvgTemp();
				break;
			case 5:
				statisticRainyDays();
				break;
			case 6:
				forecastWeather();
				break;			
			case 0:
				break;
			default:			
				break;
			}

		} while (op > 0);

	}

	private static void forecastWeather() throws IOException,
			ClassNotFoundException, InterruptedException {		
		
		Path input = new Path(defaultInputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + defaultInputPath);
			return;
		}
		Job job = Job.getInstance(conf, "forecast");
		job.setJarByClass(WeatherForecast.class);
		job.setMapperClass(WeatherForecast.WeatherMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, input);
		TableMapReduceUtil.initTableReducerJob(FORECAST,
				WeatherForecast.ForecastReducer.class, job);		
		// 明天
		String date = new SimpleDateFormat("yyyyMMdd").format(new Date(System
				.currentTimeMillis() + 24 * 60 * 60 * 1000));
		job.getConfiguration().set("date", date);
		
		if (job.waitForCompletion(true))// 提交作业
		{
			scanForecast(date);
		}
	}

	private static void scanForecast(String date) throws IOException {

		Connection conn = ConnectionFactory.createConnection(conf);
		Table tb = conn.getTable(TableName.valueOf(FORECAST));
		Scan scan = new Scan();
		scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("item"), Bytes
				.toBytes("date"), CompareOp.EQUAL, Bytes.toBytes(date)));
		ResultScanner rs = tb.getScanner(scan);
		for (Result result : rs) {
			
			String stn = Bytes.toString(result.getRow()).substring(0,6);
			System.out.println(String.format(
					"预计站点 %s 在 %s 平均气温(华氏度):%.1f\t最高气温(华氏度):%.1f\t最低气温(华氏度):%.1f",
					stn,
					Bytes.toString(result.getValue(Bytes.toBytes("item"),
							Bytes.toBytes("date"))),
					Bytes.toDouble(result.getValue(Bytes.toBytes("item"),
							Bytes.toBytes("temp"))),
					Bytes.toDouble(result.getValue(Bytes.toBytes("item"),
							Bytes.toBytes("max"))),
					Bytes.toDouble(result.getValue(Bytes.toBytes("item"),
							Bytes.toBytes("min")))));
		}
		conn.close();
	}

	private static void statisticRainyDays() throws IOException,
			ClassNotFoundException, InterruptedException {

		Path input = new Path(defaultInputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + defaultInputPath);
			return;
		}
		Job job = Job.getInstance(conf, "rainy days");
		job.setJarByClass(RainyDaysCount.class);
		job.setMapperClass(RainyDaysCount.RainyDaysMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		TableMapReduceUtil.initTableReducerJob(RESULTS,
				RainyDaysCount.IntSumReducer.class, job);

		if (job.waitForCompletion(true))// 提交作业
		{
			scanResults("statistic", "rain");
		}
	}

	private static void statisticAvgTemp() throws IOException,
			ClassNotFoundException, InterruptedException {

		Path input = new Path(defaultInputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + defaultInputPath);
			return;
		}
		Job job = Job.getInstance(conf, "avg temperature");
		job.setJarByClass(AvgTemperature.class);
		job.setMapperClass(AvgTemperature.AvgTemperatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, input);
		TableMapReduceUtil.initTableReducerJob(RESULTS,
				AvgTemperature.AvgTemperatureReducer.class, job);

		if (job.waitForCompletion(true))// 提交作业
		{
			scanResults("statistic", "avg");
		}

	}

	private static void statisticMinTemp() throws IOException,
			ClassNotFoundException, InterruptedException {

		Path input = new Path(defaultInputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + defaultInputPath);
			return;
		}
		Job job = Job.getInstance(conf, "min temperature");
		job.setJarByClass(MinTemperature.class);
		job.setMapperClass(MinTemperature.MinTemperatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, input);
		TableMapReduceUtil.initTableReducerJob(RESULTS,
				MinTemperature.MinTemperatureReducer.class, job);

		if (job.waitForCompletion(true))// 提交作业
		{
			scanResults("statistic", "min");
		}

	}

	/**
	 * 统计每年度最高气温
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void statisticMaxTemp() throws IOException,
			ClassNotFoundException, InterruptedException {

		Path input = new Path(defaultInputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + defaultInputPath);
			return;
		}
		Job job = Job.getInstance(conf, "max temperature");
		job.setJarByClass(MaxTemperature.class);
		job.setMapperClass(MaxTemperature.MaxTemperatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, input);
		TableMapReduceUtil.initTableReducerJob(RESULTS,
				MaxTemperature.MaxTemperatureReducer.class, job);

		if (job.waitForCompletion(true))// 提交作业
		{
			scanResults("statistic", "max");
		}

	}

	private static void scanResults(String family, String qualifier)
			throws IOException {

		Connection conn = ConnectionFactory.createConnection(conf);
		Table tb = conn.getTable(TableName.valueOf(RESULTS));
		ResultScanner rs = tb.getScanner(Bytes.toBytes(family),
				Bytes.toBytes(qualifier));
		for (Result result : rs) {
			String tag = "";

			if (qualifier.equals("rain")) {
				tag = "雨天总数:";
				System.out.println(String.format("年份:%s\t%s%d",
						Bytes.toString(result.getRow()).substring(6), tag,
						Bytes.toInt(result.getValue(Bytes.toBytes(family),
								Bytes.toBytes(qualifier)))));
			} else {
				if (qualifier.equals("max")) {
					tag = "最高气温(华氏度):";
				} else if (qualifier.equals("min")) {
					tag = "最低气温(华氏度):";
				} else if (qualifier.equals("avg")) {
					tag = "平均气温(华氏度):";
				} else if (qualifier.equals("rain")) {
					tag = "雨天总数:";
				}
				System.out.println(String.format("年份:%s\t%s%.1f", Bytes
						.toString(result.getRow()).substring(6), tag, Bytes
						.toDouble(result.getValue(Bytes.toBytes(family),
								Bytes.toBytes(qualifier)))));
			}

		}
		conn.close();
	}

	/**
	 * 初始化数据
	 * 
	 * @throws IOException
	 */
	private static void init() throws IOException {

		System.out.println("初始化中...");

		if (createTable(WEATHERS, WEATHERS_FAMILY)) {
			System.out.print("请输入天气数据文件所在位置(HDFS):");
			Scanner scan = new Scanner(System.in);
			try {
				importData(scan.nextLine());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}// 初始化数据

		}
		createTable(RESULTS, RESULTS_FAMILY);
		createTable(FORECAST, FORECAST_FAMILY);

	}

	/**
	 * 建表
	 * 
	 * @param 表名
	 * @param 列族
	 * @return 是否成功
	 * @throws IOException
	 */
	private static boolean createTable(String tbname, String family)
			throws IOException {
		String[] strs = family.split(",");
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		if (!admin.tableExists(TableName.valueOf(tbname))) {
			System.out.println("正在创建数据库:" + tbname);
			HTableDescriptor htable = new HTableDescriptor(
					TableName.valueOf(tbname));
			for (int i = 0; i < strs.length; i++) {
				htable.addFamily(new HColumnDescriptor(strs[i]));
			}
			admin.createTable(htable);
			System.out.println("数据库:" + tbname + " 创建完成");
			return true;
		}
		conn.close();
		return false;
	}

	/**
	 * 导入天气数据
	 * 
	 * @param inputPath
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void importData(String inputPath) throws IOException,
			ClassNotFoundException, InterruptedException {
		Path input = new Path(inputPath);
		if (!FileSystem.get(conf).exists(input)) {
			System.out.println("路径不存在:" + inputPath);
			return;
		}
		defaultInputPath = inputPath;
		conf.setStrings("io.serializations", conf.get("io.serializations"),
				MutationSerialization.class.getName(),
				ResultSerialization.class.getName(),
				KeyValueSerialization.class.getName());
		Job job = Job.getInstance(conf, "import data");
		job.setJarByClass(ImporterJob.class);
		job.setMapperClass(DataImportMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, WEATHERS);
		FileInputFormat.addInputPath(job, input);
		// 为任务添加HBase依赖库
		TableMapReduceUtil.addDependencyJars(job);

		job.waitForCompletion(true);// 提交作业
	}

	/**
	 * 获取某一天的天气数据
	 * 
	 * @throws IOException
	 */
	private static void getWeather() throws IOException {
		System.out.println("请输入日期:(yyyyMMdd)");
		Scanner input = new Scanner(System.in);
		String date = input.next();

		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(WEATHERS));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info"));
		// 设置过滤器,条件为列info:yearmoda的单元值与date相等
		scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes
				.toBytes("YEARMODA"), CompareOp.EQUAL, Bytes.toBytes(date)));

		ResultScanner rs = table.getScanner(scan);
		for (Result row : rs) {
			byte[] stn = row.getValue(Bytes.toBytes("info"),
					Bytes.toBytes("STN"));
			byte[] yearmoda = row.getValue(Bytes.toBytes("info"),
					Bytes.toBytes("YEARMODA"));
			byte[] temp = row.getValue(Bytes.toBytes("info"),
					Bytes.toBytes("TEMP"));
			byte[] max = row.getValue(Bytes.toBytes("info"),
					Bytes.toBytes("MAX"));
			byte[] min = row.getValue(Bytes.toBytes("info"),
					Bytes.toBytes("MIN"));

			System.out.println("站点:" + Bytes.toInt(stn) + "\t日期:"
					+ Bytes.toString(yearmoda) + "\t平均气温:"
					+ Bytes.toDouble(temp) + "\t最高气温:" + Bytes.toDouble(max)
					+ "\t最低气温:" + Bytes.toDouble(min));

		}
		conn.close();

	}
}
