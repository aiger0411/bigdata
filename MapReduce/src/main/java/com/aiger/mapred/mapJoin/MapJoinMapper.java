package com.aiger.mapred.mapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 1. 获取缓冲文件
        URI[] cacheFiles = context.getCacheFiles();

        // 2. 获取文件系统对象，获取输入流；
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFiles[0]));

        // 3. 读取一行；
        BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream, "utf-8"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }

        // 4. 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("\t");

        //获取pname
        String pname = pdMap.get(split[1]);

        // 封装
        outK.set(split[0] + "\t" + pname + "\t" + split[2]);

        // 输出；
        context.write(outK , NullWritable.get());


    }
}
