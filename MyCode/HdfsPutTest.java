package com.put.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 从本地上传文件到HDFS
 *
 * @author Summer
 * @version 1.0
 */

public class HdfsPutTest {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //1.客户端加载配置文件
        Configuration conf = new Configuration();

        //2.构造客户端
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.71.11:9000"),conf,"root");

        //3.上传文件
        fs.copyFromLocalFile(new Path("I:/in/a.txt"),new Path("/in"));

        //4.关闭资源
        fs.close();
    }

}
