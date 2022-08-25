package com.aiger.mapred.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 获取一行数据；
        String line = value.toString();

        // 2. 切割；
        String[] words = line.split(" ");

        // 3. 输出
        for (String word : words) {
            outK.set(word);
            context.write(outK, outV);
        }
    }
}
