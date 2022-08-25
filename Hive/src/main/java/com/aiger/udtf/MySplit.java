package com.aiger.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MySplit extends GenericUDTF {

    //定义写出的集合；
    private ArrayList<String> outList = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 1. 定义输出数据的默认列名；
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("word");

        // 2. 定义输出数据的类型；
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public void process(Object[] objects) throws HiveException {

        // 1. 获取原始数据；
        String arg = objects[0].toString();

        // 2. 获取数据传入的第二个参数，为分隔符；
        String splitKey = objects[1].toString();

        // 3. 将原始数据按照传入的分隔符进行切分；
        String[] words = arg.split(splitKey);

        // 4. 遍历切分后的结果，并写出；
        for (String word : words) {

            // 4.1 集合为复用的，首先清空集合；
            outList.clear();

            // 4.2 将每一个单词添加至集合
            outList.add(word);

            // 4.2 将集合内容写出；
            forward(outList);
        }

    }

    public void close() throws HiveException {

    }
}
