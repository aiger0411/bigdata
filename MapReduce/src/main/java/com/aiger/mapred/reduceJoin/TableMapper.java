package com.aiger.mapred.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    // 重写setup方法，获取split对象，进而获取文件名称；
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();

        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行;
        String line = value.toString();

        if (filename.contains("order")) {

            // 处理order文件的数据；
            String[] split = line.split("\t");
            outK.set(split[1]);

            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {

            // 处理pd文件的数据；
            String[] split = line.split("\t");
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        context.write(outK, outV);
    }
}
