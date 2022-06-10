package com.cpy.fowlget.main;



import com.cpy.fowlget.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * 处理ruduce驱动逻辑
 * 分析泛型
 *  Text ： 输入的key 也就是mapper阶段的type
 *  FlowBean ：  对应多条数据封装形成的bean对象
 *  Text ：传出的key 根据要求定义为 type
 *  Text ： 传出的value 由于既要输出流量总和又要输出address，所以统一转换为Text方便存储
 */
public class FlowReduce extends Reducer<Text, FlowBean,Text , Text> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, Text>.Context context) throws IOException, InterruptedException {
//        数据处理
//        定义一个Hashset存储遍历网站的地址，set不可重复，刚好不会存储相同的地址
        Set<String> addresses = new HashSet<>();
//       每个type的流量总和
        long sum = 0;
//        迭代每一个type中的所有flow
        for(FlowBean flow : values){
            addresses.add(flow.getAddress());
            sum += flow.getSumFlow();
        }
//        拼接数据
        StringBuffer stringBuffer = new StringBuffer();
        for (String str : addresses){
            stringBuffer.append(str).append(",");
        }
        stringBuffer.append("   ");
        stringBuffer.append("总数");
        stringBuffer.append(addresses.size());
        stringBuffer.append("个");
        String str = stringBuffer.toString();
//        将数据输出
        context.write(new Text(key),new Text(str));
        context.write(new Text(key), new Text(String.valueOf(sum)));
    }
}
