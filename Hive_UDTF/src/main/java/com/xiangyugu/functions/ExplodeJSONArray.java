package com.xiangyugu.functions;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ExplodeJSONArray
 * @Desciption
 * @Author XiangYu
 * @Date 2020/9/19 11:26
 * @Version 1.0
 **/
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("ExplodeJSONArray 只需要一个参数");
        }

        if ( !"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("json_array_to_struct_array的第1个参数应为string类型");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {

        String jsonArray = objects[0].toString();

        //将String转换为Json数组
        JSONArray actions = new JSONArray(jsonArray);

        //循环取出数组中的Json
        for (int i = 0; i < actions.length(); i++) {
            String[] result = new String[1];
            result[0] = actions.getString(i);
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
