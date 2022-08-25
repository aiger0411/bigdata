package com.aiger.mapred.WordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 1. 累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 2. 输出
        outV.set(sum);
        context.write(key, outV);
    }
}
