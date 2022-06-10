package com.cpy.numbercount.main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumReduce extends Reducer<Text,IntWritable,IntWritable, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable i :
                values) {
            context.write(new IntWritable(count), i);
            count++;
        }
    }
}

