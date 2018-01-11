package com.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Created by 123 on 2017/12/29.
 */

public class HBaseToHDFS {

    public class MyMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();

            for (Cell cell : cells) {
                System.out.println(cell + "==================================");
                String outValue = String.format("RowKey:%s Family:%s Qualifier:%s cellValue:%s", Bytes.toString(key.get()),Bytes.toString(CellUtil.cloneFamily(cell)),Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
                context.write(new Text(CellUtil.getCellKeyAsString(cell)), new Text(outValue));
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();

        conf.set(TableInputFormat.INPUT_TABLE, "music");

        conf.set(TableInputFormat.SCAN_COLUMNS, "info:name info:gender");

        Job job = Job.getInstance(conf, "hbase-mapreduce");

        job.setJarByClass(HBaseToHDFS.class);

        job.setInputFormatClass(TableInputFormat.class);

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.addDependencyJars(job);

        Path output = new Path("/output99/music");

        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
