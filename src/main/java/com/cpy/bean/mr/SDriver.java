package com.cpy.bean.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class SDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //        调用方法删除文件
        delFile(new File("C:\\Users\\小宇\\IdeaProjects\\maprduce_text01\\src\\main\\java\\com\\cpy\\out"));
        args = new String[]{"C:\\Users\\小宇\\IdeaProjects\\maprduce_text01\\src\\main\\java\\com\\cpy\\input\\Text",
                "C:\\Users\\小宇\\IdeaProjects\\maprduce_text01\\src\\main\\java\\com\\cpy\\out"};
//       1 创建配置对象
        Configuration config = new Configuration();
//       2 .初始化作业对象 工作空间
        Job job = Job.getInstance(config);
//       3.设置当前作业
        job.setJarByClass(SDriver.class);
//       4.设置当前作业的mapclass\
        job.setMapperClass(SMap.class);
//       5.设置作业的reduce类
        job.setReducerClass(SReduce.class);
//       6.设置map类的输出类型（对应maper）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Score.class);
//        设置数据的读写格式
        Path inputpath = new Path(args[0]);
        Path outputpath = new Path(args[1]);
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);
        boolean result = job.waitForCompletion(true);
        System.out.println(result? "chenggong": "shibai");

    }
    public static void delFile(File index) {
        if (index.isDirectory()) {
            File[] files = index.listFiles();
            for (File in : files) {
                delFile(in);
            }
        }
        index.delete();
        //出现几次删除成功代表有几个文件和文本文件
        System.out.println("删除成功");
    }
}
