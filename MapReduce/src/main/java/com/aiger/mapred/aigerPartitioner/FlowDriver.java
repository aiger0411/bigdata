package com.aiger.mapred.aigerPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FlowBean.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 链接分区类
        job.setPartitionerClass(ProvincePartitioner.class);

        // 设置ReduceTask的个数；
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("D:\\javastudyfiles\\aigerlib\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\javastudyfiles\\aigerlib\\outputPartitioner"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
