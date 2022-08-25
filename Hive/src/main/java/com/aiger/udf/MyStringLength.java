package com.aiger.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义UDF函数，需要继承GenericUDFDF类；
 * 需求：计算指定字符串的方法；
 */
public class MyStringLength extends GenericUDF {

    // 初始化方法，常用来校验参数是否合理；
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        //判断输入参数的个数；
        if(objectInspectors.length != 1){
            throw new UDFArgumentException("Input Args Length Error!");
        }

        //判断输入参数的类型；
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentException("Input Args Type Error!");
        }

        //函数本身返回值为int，需要返回int类型的鉴别器对象；
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    //处理具体的业务逻辑；
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        // 1. 判断参数是否为null；防止空指针异常；
        if (deferredObjects[0].get() == null){
            return 0;
        }

        //2. 通过get方法得到参数的值，然后通过tostring方法转换为字符串，调用length方法获取长度；
        return deferredObjects[0].get().toString().length();
    }



    public String getDisplayString(String[] strings) {
        return "";
    }
}
