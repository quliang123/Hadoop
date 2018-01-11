package cn.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

/**
 * 
 * 在不使用MapWritable时,也可以自定义数据类型实现.
 * 参考WeatherForecastBK中的用法.相应在作业驱动类中使用:
 * 		job.setMapOutputValueClass(TemperaturesWritable.class)
 * 注意需要将该类(或项目jar包)加入到$HADOOP_CLASSPATH中,否则会找不到该类.
 * 
 */
public class TemperaturesWritable implements Writable {
	private double temp;
	private double max;
	private double min;
	private String yearmoda;//yyyyMMdd
	private String stn;

	public TemperaturesWritable() {
		super();
	}

	public TemperaturesWritable(double temp, double max, double min,
			String yearmoda, String stn) {
		this.temp = temp;
		this.max = max;
		this.min = min;
		this.yearmoda = yearmoda;
		this.stn = stn;
	}

	public String getStn() {
		return stn;
	}

	public void setStn(String stn) {
		this.stn = stn;
	}

	public double getTemp() {
		return temp;
	}

	public void setTemp(double temp) {
		this.temp = temp;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	
	public String getYearmoda() {
		return yearmoda;
	}

	public void setYearmoda(String yearmoda) {
		this.yearmoda = yearmoda;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(temp);
		out.writeDouble(max);
		out.writeDouble(min);
		out.writeChars(yearmoda);
		out.writeChars(stn);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.temp = in.readDouble();
		this.max = in.readDouble();
		this.min = in.readDouble();
		this.yearmoda = in.readLine();
		this.stn = in.readLine();
	}
}