package com.aiger.mapred.aigerPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {

        // 1. 获取手机号；
        String phone = text.toString();

        // 2. 获取手机号的前3位；
        String prePhone = phone.substring(0, 3);

        // 3. 分区；
        int partition;
        if ("136".equals(prePhone)){
            partition = 0;
        }else if ("137".equals(prePhone)){
            partition = 1;
        }else if ("138".equals(prePhone)){
            partition = 2;
        }
        else if ("139".equals(prePhone)){
            partition = 3;
        }else {
            partition = 4;
        }

        return partition;
    }
}
