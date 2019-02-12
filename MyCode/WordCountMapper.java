package com.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    //对数据进行打散
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.接入数据
        String line = value.toString();
        //2.对数据进行切分
        String[] words = line.split(" ");
        //3.写出<k,v>
        for(String w:words){
            //写出reducer端
            context.write(new Text(w),new IntWritable(1));
        }
    }
}
