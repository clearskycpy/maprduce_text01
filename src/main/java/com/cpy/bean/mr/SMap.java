package com.cpy.bean.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SMap extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        System.out.println(key);
        System.out.println(value);
        String str = value.toString();
        String[] bean = str.split(" ");
//        提取所需数据
        System.out.println(bean.length);
        String name = bean[0];
        String num = bean[1];
//        进行数据封装
        int nums = Integer.parseInt(num);
        context.write(new Text(name),new IntWritable(nums));
    }
}
