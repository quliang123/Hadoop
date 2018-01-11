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
public class AvgTemperature {

	/**
	 * 从HDFS文件中获取输入
	 * 输入:<行的偏移量,行数据>
	 * 输出:<YEAR,TEMP>
	 *
	 */
	public static class AvgTemperatureMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			if(line.startsWith("STN"))
				return;
			String STN = line.substring(0, 6).trim();
			String YEAR = line.substring(14, 18).trim();
			String TEMP = line.substring(24, 28).trim();
			context.write(new Text(STN+YEAR), new DoubleWritable(Double.parseDouble(TEMP)));
		}
		

	}

	/**
	 * 汇总年度每日最高气温,求出平均值,最终结果使用HBase result表存储
	 * 输入:<YEAR,[TEMP1,TEMP2,...]>
	 * 输出:<YEAR,avg(TEMP1,TEMP2,...)>
	 *
	 */
	public static class AvgTemperatureReducer extends
			TableReducer<Text, DoubleWritable, Text> {

		@Override
		protected void reduce(Text keyIn, Iterable<DoubleWritable> valueIn,Context context)
				throws IOException, InterruptedException {
			double avgValue=0;//年度平均温度
			double sumValue=0;//年度总和
			int count=0;
			for(DoubleWritable temp:valueIn){
				sumValue+=temp.get();
				count++;
			}
			avgValue=sumValue/count;
			Put put=new Put(Bytes.toBytes(keyIn.toString()));
			put.addColumn(Bytes.toBytes("statistic"),
					Bytes.toBytes("avg"), 
					Bytes.toBytes(avgValue));
			context.write(keyIn, put);			
		}		
	}
}
