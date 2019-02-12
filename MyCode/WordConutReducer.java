package com.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordConutReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    //key->单词 values->次数
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.记录出现的次数
        int sum = 0;
        for (IntWritable v:values){
            sum += v.get();
        }
        //2.累加求和输出
        context.write(key,new IntWritable(sum));
    }
}
