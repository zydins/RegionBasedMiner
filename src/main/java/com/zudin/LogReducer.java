package com.zudin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */
public class LogReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Just translate values into one string
        ArrayList<String> list = new ArrayList<String>();
        for (Text value : values) {
              list.add(value.toString());

        }
        String[] arr = Arrays.copyOf(list.toArray(), list.size(), String[].class);
        Arrays.sort(arr, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int i1 = Integer.parseInt(o1.split(" ")[0]);
                int i2 = Integer.parseInt(o2.split(" ")[0]);
                return i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
            }
        });
        StringBuilder fullLog = new StringBuilder();
        for (String str : arr) {
            fullLog.append(str.split(" ")[1]);
            fullLog.append(";");
        }
        String res = fullLog.toString();
        RegionBasedMiner.cases.add(new LogCase(key.get(), res, arr.length)); //delete or serialize
        RegionBasedMiner.net.addSequence(res); //delete
        context.write(key, new Text(res));
    }
}
