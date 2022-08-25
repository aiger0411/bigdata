package com.aiger.mapred.logOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FileSystem fileSystem;
    private FSDataOutputStream atguiguStream;
    private FSDataOutputStream otherLog;

    public LogRecordWriter(TaskAttemptContext job) {

        //创建2个输出流，分别指向不同的路径；
        try {
            // 获取文件系统对象；
            fileSystem = FileSystem.get(job.getConfiguration());
            // 通过文件系统对象，创建2条流；
            atguiguStream = fileSystem.create(new Path("D:\\javastudyfiles\\aigerlib\\outputLog\\atguiguLog"));
            otherLog = fileSystem.create(new Path("D:\\javastudyfiles\\aigerlib\\outputLog\\otherLog"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        String line = key.toString();
        if (line.contains("atguigu")) {
            atguiguStream.writeBytes(line + "\n");
        } else {
            otherLog.writeBytes(line + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext job) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguStream);
        IOUtils.closeStream(otherLog);

    }
}
