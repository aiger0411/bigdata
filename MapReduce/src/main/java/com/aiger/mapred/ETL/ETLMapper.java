package com.aiger.mapred.ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable , Text , Text , NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (this.parseLog(line , context)) {
            context.write(value , NullWritable.get());
        }else {
            return;
        }
    }

    private boolean parseLog(String line , Context context){
        String[] split = line.split(" ");
        if (split.length > 11){
            return true;
        }else {
            return false;
        }
    }

}
