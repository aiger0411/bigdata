package com.aiger.mapred.partitionerAndWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartirioner2 extends Partitioner<Flowbean, Text> {
    @Override
    public int getPartition(Flowbean flowbean, Text text, int i) {

        //1. 获取手机号
        String phone = text.toString();

        //2. 获取手机号的前三位
        String prePhone = phone.substring(0, 3);

        //3. 分区代码
        if ("136".equals(prePhone)){
            return 0;
        }else if ("137".equals(prePhone)){
            return 1;
        }else if ("138".equals(prePhone)){
            return 2;
        }else if ("139".equals(prePhone)){
            return 3;
        }else {
            return 4;
        }
    }
}
