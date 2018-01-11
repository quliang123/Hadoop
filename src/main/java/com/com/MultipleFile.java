package com.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by 123 on 2017/12/17.
 */

public class MultipleFile {

    public static class MusicComparator extends IntWritable.Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

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


        public void reduce(Object obj, Iterable iterable, Context context)
                throws IOException, InterruptedException {
            reduce((Text) obj, iterable, context);
        }

        private IntSumReducer() {
            result = new IntWritable();
        }
    }

    public static class TokenizerMapper extends Mapper {

        private static final IntWritable one = new IntWritable(1);


        private Text word;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            //获取该行内容的迭代器
            StringTokenizer itr = new StringTokenizer(value.toString());

           /* for (; itr.hasMoreTokens();)
                word.set(itr.nextToken());
            if (word.toString().indexOf("song") != -1) context.write(word, one);*/
            while (itr.hasMoreTokens()) {
                // String[] all = itr.nextToken().split(" ");
                word.set(itr.nextToken());
                if (word.toString().contains("sing")) {   //.indexOf("song")
                    context.write(word, one);
                }
            }

        }

        public void map(Object obj, Object obj1, Context context)
                throws IOException, InterruptedException {
            map(obj, (Text) obj1, context);
        }


        public TokenizerMapper() {
            word = new Text();

        }


    }

    /**
     * 分区
     */
    public static class partition extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            String song = text.toString().substring(4);   // 拿到的数据就是  1   2    3    4等
            if (Integer.valueOf(song) < 5) {   // 小于五放在零分区
                return 0;
            }
            return 1;
        }
    }


    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static void main(String args[])
            throws Exception {
        Configuration conf = new Configuration();

        String otherArgs[] = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Music count");

        job.setJarByClass(MultipleFile.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);



        job.setPartitionerClass(partition.class);
        job.setSortComparatorClass(MusicComparator.class);

        //Type mismatch in key from map: expected org.apache.hadoop.io.Text, received org.apache.hadoop.io.LongWritable  之后添加的

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; i++)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}