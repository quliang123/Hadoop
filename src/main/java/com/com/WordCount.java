package com.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by 123 on 2017/12/17.
 */

public class WordCount
{
    public static class IntSumReducer extends Reducer
    {

        private IntWritable result;

        public void reduce(Text key, Iterable values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (Iterator i$ = values.iterator(); i$.hasNext();)
            {
                IntWritable val = (IntWritable)i$.next();
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }

        public void reduce(Object obj, Iterable iterable, Context context)
                throws IOException, InterruptedException
        {
            reduce((Text)obj, iterable, context);
        }

        public IntSumReducer()
        {
            result = new IntWritable();
        }
    }

    public static class TokenizerMapper extends Mapper
    {

        private static final IntWritable one = new IntWritable(1);
        private Text word;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            for (StringTokenizer itr = new StringTokenizer(value.toString()); itr.hasMoreTokens(); context.write(word, one))
                word.set(itr.nextToken());

        }

        public void map(Object obj, Object obj1, Context context)
                throws IOException, InterruptedException
        {
            map(obj, (Text)obj1, context);
        }


        public TokenizerMapper()
        {
            word = new Text();
        }
    }


    public WordCount()
    {
    }

    public static void main(String args[])
            throws Exception
    {
        Configuration conf = new Configuration();

        String otherArgs[] = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2)
        {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Type mismatch in key from map: expected org.apache.hadoop.io.Text, received org.apache.hadoop.io.LongWritable  之后添加的

          job.setMapOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);



        for (int i = 0; i < otherArgs.length - 1; i++)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
           FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
