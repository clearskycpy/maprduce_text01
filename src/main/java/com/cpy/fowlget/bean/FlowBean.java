package com.cpy.fowlget.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装数据的类
 * 由于使用了MapReduce框架 所以这个类需要实现Writable接口实现序列化和反序列化，同时为这个类定义比较规则
 * 所以需要实现comparable接口
 * 重写构方法和get set方法toString 还有序列化反序列化方法
 */
public class FlowBean implements Writable,Comparable{
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    private String address;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public FlowBean(long upFlow, long downFlow, String address) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = this.upFlow + this.downFlow;
        this.address = address;
    }

    public FlowBean() {
    }

    /**
     * 定义排序规则
     * @param o
     * @return
     */
    @Override
    public int compareTo(Object o) {
        FlowBean flowBean = (FlowBean)o;
        return this.sumFlow - flowBean.sumFlow > 0 ? -1 : 1;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
        dataOutput.writeUTF(address);
//        dataOutput.writeChars(address);

    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
//        address = dataInput.readLine();
        address = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                ", address='" + address + '\'' +
                '}';
    }
}
