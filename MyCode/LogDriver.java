package com.log.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Job任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.指定jar包位置
        job.setJarByClass(LogDriver.class);

        //3.关联Mapper类
        job.setMapperClass(LogMapperWordCount.class);

        //4.关联Reducer类
        job.setReducerClass(LogReducerWordCount.class);

        //5.设置mapper阶段输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //6.设置reducer阶段输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //7.设置读取数据切片的类
        job.setInputFormatClass(CombineTextInputFormat.class);

        //8.最大切片大小8M
        CombineTextInputFormat.setMaxInputSplitSize(job,8388608);

        //9.最小切片大小6M
        CombineTextInputFormat.setMinInputSplitSize(job,6291456);

        //10.设置数据输入的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //11.设置数据输出的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //12.提交任务
        boolean rs = job.waitForCompletion(true);
        System.exit(rs?0:1);

    }
}
