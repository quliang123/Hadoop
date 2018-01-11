package com.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by lenovo on 2017/12/29.
 */
public class songCount {
    static class MyMapper extends TableMapper<Text, IntWritable> {

        /**
         * @param key     rowkey   字节数组
         * @param value   列族
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
       /* @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        }*/
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            String word;
            for (Cell cell : cells) {
                String outValue = String.format(
                        "RowKey:%s Family:%s Qualifier:%s cellValue:%s ",
                        Bytes.toString(key.get()),  //行键
                        Bytes.toString(CellUtil.cloneFamily(cell)), //列族
                        Bytes.toString(CellUtil.cloneQualifier(cell)), //列修饰符
                        word = Bytes.toString(CellUtil.cloneValue(cell)) //值
                );
                // context.write(new Text(CellUtil.getCellKeyAsString(cell)), new Text(outValue));
                String[] ars = word.toString().split(" ");
                String a = word.toString().split(" ")[0];//
                System.out.println("===========================" + a);
                if (a.contains("song")) {
                    context.write(new Text(a), new IntWritable(1));
                }
            }
          /*   @Override
        protected void map(IntWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            String word;
            for (Cell cell : cells) {
                String outValue = String.format(
                        "RowKey:%s Family:%s Qualifier:%s cellValue:%s ",
                        Bytes.toString(value.get()),  //行键
                        Bytes.toString(CellUtil.cloneFamily(cell)), //列族
                        Bytes.toString(CellUtil.cloneQualifier(cell)), //列修饰符
                        word = Bytes.toString(CellUtil.cloneValue(cell)) //值
                );
                // context.write(new Text(CellUtil.getCellKeyAsString(cell)), new Text(outValue));
                String[] ars = word.toString().split(" ");
                String a = word.toString().split(" ")[0];//
                System.out.println("===========================" + a);
                if (a.contains("song")) {
                    context.write(new Text(a), new IntWritable(1));
                }
                // context.write(t, new IntWritable(1));
                *//*Text t = null;

                StringTokenizer str = new StringTokenizer(value.toString());
                while (str.hasMoreTokens()) {
                    t.set(str.nextToken());
                    if (t.toString().contains("song")) {
                        context.write(t, new IntWritable(1));
                    }
                }*//*
            }
        }*/
        }

    }

    static class MyComparator extends IntWritable.Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

     static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      /*  @Override
        protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Iterator i$ = values.iterator(); i$.hasNext(); ) {
                IntWritable val = (IntWritable) i$.next();
                sum += val.get();
            }
            context.write(new IntWritable(sum), key);
        }*/

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Iterator i$ = values.iterator(); i$.hasNext(); ) {
                IntWritable val = (IntWritable) i$.next();
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
            //super.reduce(key, values, context);
        }

      /*@Override
        protected void reduce(Text key, Iterable values, Context context) throws IOException, InterruptedException {
            int sum = 0;
           *//* for (IntWritable $iter=values.iterator();$iter.iterator().hasNext()) {
                sum += writable.get();
            }*//*
            for (Iterator i$ = values.iterator(); i$.hasNext();) {

            }
           *//* for (Iterable itr = (Iterable) values.iterator(); itr.iterator().hasNext(); ) {
                IntWritable num = (IntWritable) itr.iterator().next();
                sum += num.get();
            }*//*
            context.write(key, new IntWritable(sum));
        }*/
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, "music");
        conf.set(TableInputFormat.SCAN_COLUMNS, "info:name info:gender");
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] ot = parser.getRemainingArgs();

        Job job = Job.getInstance(conf, "hbase-mapreduce");
        job.setJarByClass(songCount.class);
        job.setInputFormatClass(TableInputFormat.class);
        job.setMapperClass(MyMapper.class);

        //   规定输出格式
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);

        //归并   只能使用器中之一
        job.setCombinerClass(MyReduce.class);
        // job.setReducerClass(MyReduce.class);

        TableMapReduceUtil.addDependencyJars(job);

        Path output = new Path("/hbaseout3/music21");
        FileOutputFormat.setOutputPath(job, output);
        /*if(job.waitForCompletion(true)){

            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setSortComparatorClass(SortComparator.class);
        }*/
    }


}
