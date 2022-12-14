package com.aiger.mapred.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComparableMapper extends Mapper<LongWritable , Text , Flowbean , Text> {

    private Flowbean outK = new Flowbean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 获取一行;
        String line = value.toString();

        // 2. 切割;
        String[] split = line.split("\t");

        // 3. 输出
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();
        outV.set(split[0]);
        context.write(outK , outV);
    }
}
