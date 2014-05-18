package com.zudin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */
public class LogMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Test input - "case_id activity_name", but actually we can parse string as we want
        String[] data = value.toString().split(" ");
        int id = Integer.parseInt(data[data.length - 1]);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < data.length - 1; i++) {
            buff.append(data[i]);
            buff.append(" ");
        }
        context.write(new IntWritable(id), new Text(buff.toString()));
    }
}
