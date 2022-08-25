package com.aiger.mapred.WordCountCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 获取配置信息，获取Job对象
        Job job = Job.getInstance(new Configuration());

        // 2. 指明jar所在的位置；
        job.setJarByClass(WordCountDriver.class);

        // 3. 链接Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 指明Mapper的输出数据类型；
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指明最终的输出数据类型；
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountReducer.class);

        // 6. 指明数据输入/输出路径；
        FileInputFormat.setInputPaths(job, new Path("D:\\javastudyfiles\\aigerlib\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\javastudyfiles\\aigerlib\\outputCombiner"));

        // 7. 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
