package cn.weather;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 统计年度最高气温
 *
 */
public class MaxTemperature {

	/**
	 * 从HDFS文件中获取输入
	 * 输入:<行的偏移量,行数据>
	 * 输出:<YEAR,MAX>
	 *
	 */
	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			if(line.startsWith("STN"))
				return;
			String STN = line.substring(0, 6).trim();
			String YEAR = line.substring(14, 18).trim();
			String MAX = line.substring(102, 108).trim();
			context.write(new Text(STN+YEAR), new DoubleWritable(Double.parseDouble(MAX)));
		}
		

	}

	/**
	 * 汇总年度每日最高气温,求出最大值,最终结果使用HBase result表存储
	 * 输入:<YEAR,[MAX1,MAX2,...]>
	 * 输出:<YEAR,max(MAX1,MAX2,...)>
	 *
	 */
	public static class MaxTemperatureReducer extends
			TableReducer<Text, DoubleWritable, Text> {

		@Override
		protected void reduce(Text keyIn, Iterable<DoubleWritable> valueIn,Context context)
				throws IOException, InterruptedException {
			double maxValue=Double.MIN_VALUE;
			for(DoubleWritable temp:valueIn){
				maxValue=Math.max(maxValue, temp.get());
			}
			Put put=new Put(Bytes.toBytes(keyIn.toString()));
			put.addColumn(Bytes.toBytes("statistic"),
					Bytes.toBytes("max"), 
					Bytes.toBytes(maxValue));
			context.write(keyIn, put);			
		}		
	}
}
