package com.aiger.mapred.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    // 该属性用来存储pd的value；
    private TableBean pdTableBean = new TableBean();

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        // 创建集合，存储order中的values
        ArrayList<TableBean> orderBeans = new ArrayList<>();

        // 遍历values
        for (TableBean value : values) {

            if ("order".equals(value.getFlag())) {

                //处理order数据
                TableBean temOrderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(temOrderBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderBeans.add(temOrderBean);
            } else {
                pdTableBean.setPname(value.getPname());
            }
        }

        // 遍历orderBeans集合
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdTableBean.getPname());
            context.write(orderBean, NullWritable.get());
        }

    }
}
