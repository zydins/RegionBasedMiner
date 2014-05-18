package com.zudin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */

public class LogMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        String[] data = value.toString().split(" ");
        int id = Integer.parseInt(data[data.length - 1]);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < data.length - 1; i++) {
            buff.append(data[i]);
            buff.append(" ");
        }
        output.collect(new IntWritable(id), new Text(buff.toString()));
    }
}
