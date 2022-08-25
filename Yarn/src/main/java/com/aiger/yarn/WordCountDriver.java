package com.aiger.yarn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class WordCountDriver {

    private static Tool tool;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 判断是否有tool接口；
        /*if ("WordCount".equals(args[0])){
            tool = new WordCount();
        }else {
            throw new RuntimeException("no such tool:" + args[0]);
        }*/
        switch (args[0]) {
            case "WordCount":
                tool = new WordCount();
                break;
            default:
                throw new RuntimeException("no such tool:" + args[0]);
        }
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}
