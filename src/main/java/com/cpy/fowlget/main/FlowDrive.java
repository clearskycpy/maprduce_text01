package com.cpy.fowlget.main;

import com.cpy.fowlget.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 主类
 */
public class FlowDrive {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        args = new String[]{"C:\\Users\\小宇\\IdeaProjects\\maprduce_text01\\src\\main\\java\\com\\cpy\\fowlget\\input\\text",
                "C:\\Users\\小宇\\IdeaProjects\\maprduce_text01\\src\\main\\java\\com\\cpy\\out"};
        //        调用方法删除文件
        delFile(new File(args[1]));
//       1 创建配置对象
        Configuration config = new Configuration();
//       2 .初始化作业对象 工作空间
        Job job = Job.getInstance(config);
//       3.设置当前作业
        job.setJarByClass(FlowDrive.class);
//       4.设置当前作业的mapclass\
        job.setMapperClass(FlowMapper.class);
//       5.设置作业的reduce类
        job.setReducerClass(FlowReduce.class);
//       6.设置map类的输出类型（对应maper）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
//        设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        设置数据的读写格式
        Path inputpath = new Path(args[0]);
        Path outputpath = new Path(args[1]);
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);
        boolean result = job.waitForCompletion(true);
        System.out.println(result? "成功": "失败");
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
