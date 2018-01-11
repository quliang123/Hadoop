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
public class MinTemperature {

	/**
	 * 从HDFS文件中获取输入
	 * 输入:<行的偏移量,行数据>
	 * 输出:<YEAR,MIN>
	 *
	 */
	public static class MinTemperatureMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			if(line.startsWith("STN"))
				return;
			String STN = line.substring(0, 6).trim();
			String YEAR = line.substring(14, 18).trim();
			String MIN = line.substring(110, 116).trim();
			context.write(new Text(STN+YEAR), new DoubleWritable(Double.parseDouble(MIN)));
		}
		

	}

	/**
	 * 汇总年度每日最低气温,求出最小值,最终结果使用HBase result表存储
	 * 输入:<YEAR,[MIN1,MIN2,...]>
	 * 输出:<YEAR,min(MIN1,MIN2,...)>
	 *
	 */
	public static class MinTemperatureReducer extends
			TableReducer<Text, DoubleWritable, Text> {

		@Override
		protected void reduce(Text keyIn, Iterable<DoubleWritable> valueIn,Context context)
				throws IOException, InterruptedException {
			double minValue=Double.MAX_VALUE;//初始为最大值.
			for(DoubleWritable temp:valueIn){
				minValue=Math.min(minValue, temp.get());
			}
			Put put=new Put(Bytes.toBytes(keyIn.toString()));
			put.addColumn(Bytes.toBytes("statistic"),
					Bytes.toBytes("min"), 
					Bytes.toBytes(minValue));
			context.write(keyIn, put);			
		}		
	}
}
