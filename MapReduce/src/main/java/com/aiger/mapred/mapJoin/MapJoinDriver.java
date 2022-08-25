package com.aiger.mapred.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MapJoinDriver.class);

        // 添加缓冲文件
        job.addCacheFile(new URI("file:///D:/javastudyfiles/aigerlib/inputCache/pd.txt"));

        // 设置ReduceTask的个数
        job.setNumReduceTasks(0);

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\javastudyfiles\\aigerlib\\inputMap"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\javastudyfiles\\aigerlib\\outputMap"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
