package com.com;// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   WordCount.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCountMulti
{
	//2.Reduce代码
	public static class IntSumReducer extends Reducer
	{
		Logger logger=Logger.getLogger("Word");
		private IntWritable result;

		public void reduce(Text key, Iterable values, Context context)
			throws IOException, InterruptedException
		{
          //统计总数
			int sum = 0;
			//i$就是一个迭代器对象
			Iterator i$ = values.iterator();
			while (i$.hasNext())
			{
				IntWritable val = (IntWritable)i$.next();
				System.out.println("================Reducer输入键值对<" + key.toString() + ","

						+val.get()+ ">");
				logger.trace("================Reducer输入键值对<" + key.toString() + ","

						+val.get()+ ">");
				//get方法将IntWritable转成int
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

		public  void reduce(Object obj, Iterable iterable, Context context)
			throws IOException, InterruptedException
		{
			reduce((Text)obj, iterable, context);
		}

		public IntSumReducer()
		{
			result = new IntWritable();
		}
	}

    //1.继承Mapper，并实现Mapper方法
	public static class TokenizerMapper extends Mapper
	{
		//保存每行每个单词的个数
		//private static final IntWritable one = new IntWritable(1);
		private Text word;
		Logger logger=Logger.getLogger("Word");
		private MultipleOutputs mos;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//读取资源
			mos = new MultipleOutputs(context);
		}
		/**
		 *
		 * @param key  从文件中获取到key
		 * @param value  当前行文本内容       单行文本
		 * @param context Map上下文 提供了map运行的环境
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException
		{
			mos.write(NullWritable.get(), value,
					generateFileName(value));
			//获取到该行内容的迭代器
			StringTokenizer itr = new StringTokenizer(value.toString());
			//其实我们所说的输出就是在上下文write时候进行的
			for (; itr.hasMoreTokens(); context.write(word, new IntWritable(1)))
			{
				word.set(itr.nextToken());
			}
			System.out.println("Mapper输出<" + word + "," + 1 + ">");
			logger.trace("Mapper输出<" + word + "," + 1 + ">");
		}
		//写到Map中
		private String generateFileName(Text value) {
         char c = value.toString().toLowerCase().charAt(0);
 		 if (c>='a'&&c<='z'){
 		 	return c+".txt";
		 }
         return "other.txt";
        }



		public  void map(Object obj, Object obj1, Context context)
			throws IOException, InterruptedException
		{
			map(obj, (Text)obj1, context);
		}


		public TokenizerMapper()
		{
			word = new Text();
		}
	}

	public WordCountMulti()
	{
	}
     //静态内部类   新APi中MutilpleOutputs
	public static class OurOutputFormat extends MultipleOutputs {
		public OurOutputFormat(TaskInputOutputContext context) {
			super(context);
		}
	}
	public static void main(String args[])
		throws Exception
	{
		//读取配置
		Configuration conf = new Configuration();
		//1.  通用的参数解析器   hadoop jar  05xxx.jar cn.happy.WordCount /input/www.txt /outputn

		//return this.commandLine == null?new String[0]:this.commandLine.getArgs(); 读取命令行中的参数，形成程序中的数组
		String otherArgs[] = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		//参数个数不能少于2个
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountMulti.class);
		job.setMapperClass(TokenizerMapper.class);

		//自定义的Reduce
		job.setReducerClass(IntSumReducer.class);
       //改变默认输出方式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < otherArgs.length - 1; i++) {
			//设置输入路径
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		//设置多文件输出
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
       //设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		//假的
		//任务发送的TaskTrack  是否打印信息
		/**
         * true:在命令行监控  任务的执行过程/阶段
		 * false:不监控输出
		 */
		job.waitForCompletion(true);
	}
}










