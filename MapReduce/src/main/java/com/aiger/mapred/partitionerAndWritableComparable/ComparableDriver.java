package com.aiger.mapred.partitionerAndWritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComparableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 获取配置信息，获取Job对象
        Job job = Job.getInstance(new Configuration());

        // 2. 指定jar包所在的位置；
        job.setJarByClass(ComparableDriver.class);

        // 3. 关联Mapper和Reducer
        job.setMapperClass(ComparableMapper.class);
        job.setReducerClass(ComparableReducer.class);

        // 4. 指定Mapper的KV类型
        job.setMapOutputKeyClass(Flowbean.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 指定最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        // 6. 指定数据的输入输出路径；
        FileInputFormat.setInputPaths(job, new Path("D:\\javastudyfiles\\aigerlib\\inputComparable"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\javastudyfiles\\aigerlib\\outputComparable3"));

        // 链接分区
        job.setPartitionerClass(ProvincePartirioner2.class);

        // 指定ReduceTask的个数
        job.setNumReduceTasks(5);

        // 7. 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
