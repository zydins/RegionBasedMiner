package com.zudin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Sergey Zudin
 * Date: 13.04.14
 */
public class LogReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        //Just translate values into one string
        ArrayList<String> list = new ArrayList<String>();
        while (values.hasNext()) {
              list.add(values.next().toString());
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
        output.collect(key, new Text(res));
    }
}
