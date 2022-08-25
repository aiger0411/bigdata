package com.aiger.mapred.partitionerAndWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ComparableReducer extends Reducer<Flowbean, Text , Text , Flowbean> {

    @Override
    protected void reduce(Flowbean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 输出；
        for (Text value : values) {
            context.write(value , key);
        }
    }
}
