package com.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by 123 on 2017/12/17.
 * 多文件输出
 */
public class File {


    public static class IntSumReducer extends Reducer {

        private IntWritable result;


        public void reduce(Text key, Iterable values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (Iterator i$ = values.iterator(); i$.hasNext(); ) {
                IntWritable val = (IntWritable) i$.next();
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }




       /* public void  getFileName(){

        }*/

        public void reduce(Object obj, Iterable iterable, Context context)
                throws IOException, InterruptedException {
            reduce((Text) obj, iterable, context);
        }

        public IntSumReducer() {
            result = new IntWritable();
        }


    }

    public static class TokenizerMapper extends Mapper {

        //多文件输出
        private MultipleOutputs mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            mos = new MultipleOutputs(context);
        }


        private static final IntWritable one = new IntWritable(1);
        private Text word;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            /**
             * aa aaa  abc  cdb
             bbbb  bac
             */
            //StringTokenizer   迭代器     dddd            dec
            for (StringTokenizer itr = new StringTokenizer(value.toString()); itr.hasMoreTokens(); context.write(word, one)) {        //
                String temp = itr.nextToken();
                word.set(temp);
                //  循环写入文件中    不能使用new NullWritable()来定义，获取空值只能NullWritable.get()来获取        temp    就是value    具体的内容            文件名
                             //key       value         baseoutputpath
                mos.write(NullWritable.get(), temp,createFileName(new Text(temp)));
            }
        }

        public void map(Object obj, Object obj1, Context context)
                throws IOException, InterruptedException {
            map(obj, (Text) obj1, context);
        }

        //创建文件名称
        private String createFileName(Text values) {
            char c = values.toString().toLowerCase().charAt(0);   //处理文件名称
            if (c <= 'z' && c >= 'a') {  //c无论如何要不然就是对应的文件
                return c + ".txt";
            }
            return "other.txt";    //否则输出到 other文件中
        }

        public TokenizerMapper() {
            word = new Text();
        }
    }

    public File() {
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
        job.setJarByClass(File.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        //Type mismatch in key from map: expected org.apache.hadoop.io.Text, received org.apache.hadoop.io.LongWritable  之后添加的

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        for (int i = 0; i < otherArgs.length - 1; i++)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
