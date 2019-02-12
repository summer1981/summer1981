package com.log.wordcount;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class LogReducerWordCount extends Reducer <Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.计数器
        long upFlow_sum = 0;
        long dfFlow_sum = 0;

        //2.
        for (FlowBean v:values){
            upFlow_sum += v.getUpFlow();
            dfFlow_sum += v.getDfFlow();
        }

        FlowBean rsSum = new FlowBean(upFlow_sum,dfFlow_sum);

        //输出结果
        context.write(key,rsSum);
    }
}
