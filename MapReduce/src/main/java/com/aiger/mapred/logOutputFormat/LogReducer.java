package com.aiger.mapred.logOutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text , NullWritable , Text , NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 为了防止有相同的key，所以遍历values
        for (NullWritable value : values) {
            context.write(key , NullWritable.get());
        }
    }
}
