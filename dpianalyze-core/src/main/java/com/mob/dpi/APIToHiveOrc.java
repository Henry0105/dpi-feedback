package com.mob.dpi;

import com.alibaba.fastjson.JSONObject;
import com.mob.dpi.pojo.ComparableList;
import com.mob.dpi.pojo.StandardStructObjectInspector;
import com.mob.dpi.utils.HDFSUtils;
import com.mob.dpi.utils.HiveUtils;
import com.mob.dpi.utils.HttpUtils;
import com.mob.dpi.utils.MD5Util;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * @Author: xuchl
 * @Date: 2022/3/2 10:16
 */
public class APIToHiveOrc {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());

    public static void main(String[] args) throws Exception {

        // 元数据格式【列名1:类型1,...列名n:类型n】
        // 必须按顺序排列
        String schema = "gwId:String,companyName:String,visitDate:String,pvSum:Int,monthPvSum:Int,companyAddress:String,deviceType:String";

        // 20220221
        String date = args[0];

        // 写入Hive表，格式【数据库.数据表】
        String hiveTable = args[1];

        try {
            TreeSet<ComparableList> rows = HttpUtils.getApiData(date);
            logger.info("{}数据拉取完成，条数{}",date,rows.size());

            StandardStructObjectInspector standardStructObjectInspector = HiveUtils.generateHiveInspector(schema);

            FileSystem fs = HDFSUtils.createFileSystem();
            HDFSUtils.saveBatchDataHDFSOrc1(fs,rows,date,standardStructObjectInspector,hiveTable);

            logger.info("{}存入hive表{}",date,hiveTable);

            // to do
            // email

        } catch (Exception e){
            e.printStackTrace();
            logger.error("{}数据拉取异常",date);

            // to do
            // email
        }


    }

}
