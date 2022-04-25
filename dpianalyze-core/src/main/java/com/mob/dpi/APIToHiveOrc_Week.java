package com.mob.dpi;

import com.mob.dpi.pojo.ComparableList;
import com.mob.dpi.pojo.StandardStructObjectInspector;
import com.mob.dpi.utils.HDFSUtils;
import com.mob.dpi.utils.HiveUtils;
import com.mob.dpi.utils.HttpUtils;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: xuchl
 * @Date: 2022/3/2 10:16
 */
public class APIToHiveOrc_Week {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());

    public static void main(String[] args) throws Exception {

        // 元数据格式【列名1:类型1,...列名n:类型n】
        // 必须按顺序排列
        String schema = "gw_id:String,company_name:String,company_address:String,visit_date:String,hour:Int,pv:Int,device_type:String," +
                "describe_1:String,describe_2:String,website_type:String";

        // 20220221
        String date = args[0];

        // 写入Hive表，格式【数据库.数据表】
        String hiveTable = args[1];


        List<ComparableList> rows = HttpUtils.getApiDataWeek(date,schema);

        if(rows.size() == 0){
            logger.error("{}数据为空",date);
            System.exit(-1);
        }

        logger.info("{}数据拉取完成，条数{}",date,rows.size());
        System.out.println("数据拉取完成，条数" + rows.size());

        StandardStructObjectInspector standardStructObjectInspector = HiveUtils.generateHiveInspector(schema);

        FileSystem fs = HDFSUtils.createFileSystem();
        HDFSUtils.saveBatchDataHDFSOrc1(fs,rows,date,standardStructObjectInspector,hiveTable);

        logger.info("{}存入hive表{}",date,hiveTable);

    }

}
