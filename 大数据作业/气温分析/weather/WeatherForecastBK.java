package cn.weather;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 预测天气,读取共享变量中的指定 日期,分析往年数据中这一天的所有平均,最高和最低气温,然后再分别求平均值
 */
public class WeatherForecastBK {

	public static class WeatherMapper extends
			Mapper<LongWritable, Text, Text, TemperaturesWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.startsWith("STN"))
				return;
			String YEAR = line.substring(14, 18).trim();
			String MODA = line.substring(18, 22).trim();
			String YEARMODA = YEAR + MODA;
			// 与配置项中日期数据比较,其中不包括年份的比较
			if (MODA.equals(context.getConfiguration().get("date").substring(4))) {
				String STN = line.substring(0, 6).trim();
				String TEMP = line.substring(24, 28).trim();
				String MAX = line.substring(102, 108).trim();
				String MIN = line.substring(110, 116).trim();
				// 输出到reduce
				context.write(new Text(STN + MODA), new TemperaturesWritable(
						Double.parseDouble(TEMP), Double.parseDouble(MAX),
						Double.parseDouble(MIN), YEARMODA, STN));
			}
		}
	}

	public static class ForecastReducer extends
			TableReducer<Text, TemperaturesWritable, Text> {

		@Override
		protected void reduce(Text keyIn,
				Iterable<TemperaturesWritable> valueIn, Context context)
				throws IOException, InterruptedException {
			double tempSum = 0;
			double maxSum = 0;
			double minSum = 0;
			int n = 0;
			for (TemperaturesWritable temperature : valueIn) {
				
				tempSum += temperature.getTemp();
				maxSum += temperature.getMax();
				minSum += temperature.getMin();
				n++;
			}
			double tempAvg = tempSum / n;
			double maxAvg = maxSum / n;
			double minAvg = minSum / n;

			String date = context.getConfiguration().get("date");
			String rowKey = keyIn.toString().substring(0, 6) + date;// 行键=STN+date
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("temp"),
					Bytes.toBytes(tempAvg));
			put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("max"),
					Bytes.toBytes(maxAvg));
			put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("min"),
					Bytes.toBytes(minAvg));
			put.addColumn(Bytes.toBytes("item"), Bytes.toBytes("date"),
					Bytes.toBytes(date));
			context.write(new Text(rowKey), put);
		}

	}

}
