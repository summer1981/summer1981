package com.log.wordcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    //定义属性：上行流量 下行流量 流量总和
    private long upFlow;
    private long dfFlow;
    private long flowSum;

    public FlowBean(){}

    public FlowBean(long upFlow,long dfFlow){
        this.upFlow = upFlow;
        this.dfFlow = dfFlow;
        this.flowSum = upFlow + dfFlow;
    }

    public  long getUpFlow(){
        return upFlow;
    }

    public void setUpFlow(long upFlow){
        this.upFlow = upFlow;
    }

    public long getDfFlow(){
        return dfFlow;
    }

    public void setDfFlow(long dfFlow){
        this.dfFlow = dfFlow;
    }

    public long getFlowSum(){
        return flowSum;
    }

    public void setFlowSum(long flowSum){
        this.flowSum = flowSum;
    }

    //序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dfFlow);
        out.writeLong(flowSum);
    }
    //反序列化
    public void readFields(DataInput in) throws IOException {
       upFlow = in.readLong();
       dfFlow = in.readLong();
       flowSum = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dfFlow + "\t" + flowSum;
    }
}
