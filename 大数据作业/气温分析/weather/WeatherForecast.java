package cn.weather;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 预测天气,读取共享变量中的指定 日期,分析往年数据中这一天的所有平均,最高和最低气温,然后再分别求平均值
 */
public class WeatherForecast {

	public static class WeatherMapper extends
			Mapper<LongWritable, Text, Text, MapWritable> {

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
				MapWritable map=new MapWritable();
				map.put(new Text("temp"), new DoubleWritable(Double.parseDouble(TEMP)));
				map.put(new Text("max"), new DoubleWritable(Double.parseDouble(MAX)));
				map.put(new Text("min"), new DoubleWritable(Double.parseDouble(MIN)));
				map.put(new Text("yearmoda"), new Text(YEARMODA));
				map.put(new Text("stn"), new Text(STN));
				// 输出到reduce
				context.write(new Text(STN + MODA),map);
			}
		}
	}

	public static class ForecastReducer extends
			TableReducer<Text, MapWritable, Text> {

		@Override
		protected void reduce(Text keyIn,
				Iterable<MapWritable> valueIn, Context context)
				throws IOException, InterruptedException {
			double tempSum = 0;
			double maxSum = 0;
			double minSum = 0;
			int n = 0;
			for (MapWritable temperature : valueIn) {
				
				tempSum +=((DoubleWritable) temperature.get(new Text("temp"))).get();
				maxSum += ((DoubleWritable) temperature.get(new Text("max"))).get();
				minSum += ((DoubleWritable) temperature.get(new Text("min"))).get();
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
