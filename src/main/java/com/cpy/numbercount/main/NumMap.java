package com.cpy.numbercount.main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NumMap extends Mapper<LongWritable, Text,Text, IntWritable> {
    final static String TYPE_ONE = "one";
    final static String TYPE_TWO = "two";
    final static String TYPE_THREE = "three";
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        System.out.println(key);
        System.out.println(value);
        String str = value.toString();
        String[] bean = str.split(",");
//        提取所需数据
        for (int i = 0; i < bean.length; i++) {
            int num = Integer.parseInt(bean[i]);
            if (num <= 100){
                //        进行数据封装
                context.write(new Text(TYPE_ONE),new IntWritable(num));
            }else if (num >100 && num <= 200){
                //        进行数据封装
                context.write(new Text(TYPE_TWO),new IntWritable(num));
            }else {
                //        进行数据封装
                context.write(new Text(TYPE_THREE),new IntWritable(num));
            }
        }

    }
}
