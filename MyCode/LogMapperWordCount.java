package com.log.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapperWordCount extends Mapper <LongWritable, Text, Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.接入数据
        String line = value.toString();

        //2.切分数据 \t
        String[] fields = line.split("\t");

        //3.封装对象，拿到关键字段:上，下行，手机号
        String phoneNr = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long dfFlow = Long.parseLong(fields[fields.length - 2]);

        //4.写出到reducer
        context.write(new Text(phoneNr),new FlowBean(upFlow,dfFlow));
    }
}
