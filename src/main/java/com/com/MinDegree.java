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
 * 统计最低气温
 */
public class MinDegree {

    /**
     * 四个泛型类型分别代表：
     * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
     * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
     * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
     * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
     */
    public static class IntSumReducer extends Reducer {

        private IntWritable result;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //int maxValue = Integer.MIN_VALUE;
            int maxValue = Integer.MAX_VALUE;
            for (IntWritable item : values) {
                //  maxValue = Math.max(maxValue, item.get());   //0             1,2,3,4,5,6
                maxValue = Math.min(maxValue, item.get());
            }

           /* for (Iterator i$ = values.iterator(); i$.hasNext(); ) {
                IntWritable val = (IntWritable) i$.next();
                sum += val.get();
            }*/
            //result.set(maxValue);
            context.write(key, new IntWritable(maxValue));
        }

        public void reduce(Object obj, Iterable iterable, Context context)
                throws IOException, InterruptedException {
            reduce((Text) obj, iterable, context);
        }

        public IntSumReducer() {
            result = new IntWritable();
        }
    }


    /**
     * 四个泛型类型分别代表：
     * * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
     * ValueIn      Mapper的输入数据的Value，这里是每行文字
     * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
     * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
     */
    public static class TokenizerMapper extends Mapper {

        private static final IntWritable one = new IntWritable(1);
        private Text word;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String data = value.toString();
            //年份
            String year = data.toString().substring(15, 19);
            int degree = 0;
            if ("+" == data.substring(43, 44)) {
                degree = Integer.parseInt(data.toString().substring(44, 49));
            } else {
                degree = Integer.parseInt(data.toString().substring(43, 49));
            }
            context.write(new Text(year), new IntWritable(degree));

        }

        public void map(Object obj, Object obj1, Context context)
                throws IOException, InterruptedException {
            map(obj, (Text) obj1, context);
        }


        public TokenizerMapper() {
            word = new Text();
        }
    }


    public MinDegree() {
    }

    public static void main(String args[])
            throws Exception {
        Configuration conf = new Configuration();

        String otherArgs[] = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MinDegree.class);
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
