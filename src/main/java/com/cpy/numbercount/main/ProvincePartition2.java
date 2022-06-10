package com.cpy.numbercount.main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartition2 extends Partitioner<Text,IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String type = text.toString();
        if ("one".equals(type)){
            return 0;
        }else if ("two".equals(type)){
            return 1;
        }else {
            return 2;
        }
    }
}
