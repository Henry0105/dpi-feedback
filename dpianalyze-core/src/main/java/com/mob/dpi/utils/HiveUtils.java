package com.mob.dpi.utils;

import com.mob.dpi.pojo.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author: xuchl
 * @Date: 2022/3/1 18:56
 */
public class HiveUtils {
    private static final Logger logger = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());

    /**
     * 生成orc构造器
     * @param schema
     */
    public static StandardStructObjectInspector generateHiveInspector(String schema){
        // 创建字段分割器StandardStructObjectInspector
        String[] fields = schema.split(",");
        List<String> structFieldNames = new ArrayList();
        List<ObjectInspector> objectInspectors = new ArrayList();

        for(String str:fields) {
            String field = str.split(":")[0];
            String type = str.split(":")[1];

            if("Boolean".equals(type)){
                structFieldNames.add(field);
                objectInspectors.add(new JavaBooleanObjectInspector());
            } else if("Date".equals(type)) {
                structFieldNames.add(field);
                objectInspectors.add(new JavaStringObjectInspector());
            } else if("Int".equals(type)) {
                structFieldNames.add(field);
                objectInspectors.add(new JavaIntObjectInspector());
            } else if("String".equals(type)) {
                structFieldNames.add(field);
                objectInspectors.add(new JavaStringObjectInspector());
            } else{
                structFieldNames.add(field);
                objectInspectors.add(new JavaStringObjectInspector());
            }
        }

        StandardStructObjectInspector standardStructObjectInspector = new StandardStructObjectInspector(structFieldNames,objectInspectors);

        return standardStructObjectInspector;
    }


    /**
     * 根据LinkedHashMap(自动排序)创建ComparableList<String>
     * 去除分区字段
     * @param rowMap
     * @return
     */
    public static ComparableList<String> createRowStringList(LinkedHashMap<String,String> rowMap,List<String> partitionColumnsList){
        ComparableList<String> row = new ComparableList<String>();

        Iterator it = rowMap.keySet().iterator();
        while(it.hasNext()){
            String column = it.next().toString();

            // 去除分区字段
            if(!partitionColumnsList.contains(column)){
                row.add(rowMap.get(column));
            }
        }

        //
        return row;
    }



}
