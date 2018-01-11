package cn.weather;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 每年雨天统计
 *
 */
public class RainyDaysCount {

	public static class RainyDaysMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.startsWith("STN"))
				return;
			String STN = line.substring(0, 6).trim();
			String YEAR = line.substring(14, 18).trim();
			String FRSHTT = line.substring(132, 138).trim();
			String rainy = "010000";// 雨天格式
			// 转为二进制,参数"2"表示转为二进制数
			int a = Integer.parseInt(FRSHTT, 2);
			int b = Integer.parseInt(rainy, 2);
			// 按位与运算,结果与rainy对应二进制数相同则输出
			if ((a & b) == b) {
				context.write(new Text(STN + YEAR), new IntWritable(1));// 计数1次
			}
		}

	}

	public static class IntSumReducer extends
			TableReducer<Text, IntWritable, Text> {

		@Override
		protected void reduce(Text keyIn, Iterable<IntWritable> valueIn,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : valueIn) {
				sum += val.get();
			}
			Put put=new Put(Bytes.toBytes(keyIn.toString()));
			put.addColumn(Bytes.toBytes("statistic"),
					Bytes.toBytes("rain"), 
					Bytes.toBytes(sum));
			context.write(keyIn, put);	
		}

	}
}
