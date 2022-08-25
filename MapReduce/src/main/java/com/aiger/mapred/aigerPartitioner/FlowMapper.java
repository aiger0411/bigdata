package com.aiger.mapred.aigerPartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 获取一行数据；
        String line = value.toString();

        // 2. 分割；
        String[] split = line.split("\t");

        // 3. 输出；
        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();
        context.write(outK, outV);
    }
}
