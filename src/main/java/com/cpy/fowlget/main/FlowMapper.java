package com.cpy.fowlget.main;


import com.cpy.fowlget.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper阶段的泛型
 *LongWritable : 输入的文件每一行的偏移量
 * Text : 每一行读取的内容
 * Text : 使用Context对象输出时候的key 这里根据案例要求定义为数据里面的数据来源（例：视频播放...）
 * FlowBean : 使用Context对象输出时候的value
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//            转换成String数组
        String str = value.toString();
//        按照，分割
        String[] bean = str.split(",");
//        3719199419,84-25-DB-4F-10-1A:CMCC-EASY,120.196.100.99,input.shouji.sogou.com,搜索引擎,21,131,248,9512,200
//        提取所需数据
//        提取类型
        String address = bean[4];
        System.out.println(address);
//        提取地址
        String type = bean[5];
        System.out.println(type);
//        提取流量
        long upFlow = Long.parseLong(bean[bean.length-3]);
        long downFlow = Long.parseLong(bean[bean.length-2]);
//        筛选无用的数据 ，只有这条数据具有类型和地址才能被传到reduce阶段进行处理
        if ((!"".equals(address) && (!"".equals(type)))) { // address != null && type != null
            FlowBean flowBean = new FlowBean(upFlow, downFlow, address);
            context.write(new Text(type), flowBean);
        }
    }
}
