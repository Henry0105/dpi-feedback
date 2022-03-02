package com.mob.dpi.utils;

import com.mob.dpi.pojo.ComparableList;
import com.mob.dpi.pojo.StandardStructObjectInspector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @Author: xuchl
 * @Date: 2022/3/2 12:21
 */
public class HDFSUtils {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Thread.currentThread().getStackTrace()[1].getClassName());

    /**
     * 文件系统连接到 hdfs的配置信息
     */
    private static Configuration getConf() {
        // 创建配置实例
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ShareSdkHadoop");

        return conf;
    }

    /**
     * 创建文件系统实例
     *
     * @return
     * @throws IOException
     */
    public static FileSystem createFileSystem() throws IOException {
        // 创建文件系统实例
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        return fs;
    }

    /**
     * 写入HDFS（ORC格式）
     *
     * @return
     */
    public static void saveOrcFile(FileSystem fs, String fileName, TreeSet<ComparableList> rows, StandardStructObjectInspector standardStructObjectInspector) throws IOException {
        JobConf conf = new JobConf();
        //FileSystem fs = HDFSUtils.createFileSystem();
        Path outputPath = new Path(fileName);

        // 构造Orc
        OrcSerde serde = new OrcSerde();

        OutputFormat outFormat = new OrcOutputFormat();
        RecordWriter writer = outFormat.getRecordWriter(fs, conf, outputPath.toString(), Reporter.NULL);

        // 遍历每一行数据
        //System.out.println(rows.size());
        Iterator it = rows.iterator();
        while(it.hasNext()) {
            List<String> list = (ComparableList) it.next();

            Writable row = serde.serialize(list, standardStructObjectInspector);
            writer.write(NullWritable.get(), row);
            //System.out.println("正常插入完毕");
        }

        writer.close(Reporter.NULL);
        //fs.close();
    }
    /**
     * 目录是否存在
     *
     * @param dir
     * @throws IOException
     */
    public static boolean isExist(FileSystem fs,String dir) throws IOException  {
        // 创建文件系统实例
        //FileSystem fs = createFileSystem();

        return fs.exists(new Path(dir));
    }


    /**
     * 存储批次文件到HDFS【一级分区】
     *
     * @param fs
     * @param rows
     * @param date
     * @param standardStructObjectInspector
     * @param hiveTable
     * @return
     * @throws Exception
     */
    public static boolean saveBatchDataHDFSOrc1(FileSystem fs, TreeSet<ComparableList> rows, String date, StandardStructObjectInspector standardStructObjectInspector, String hiveTable) throws Exception {
        // 获取Hive表存储目录
        String hiveTablePath = "/user/hive/warehouse/".concat(hiveTable.replace(".",".db/"));
        logger.info("Hive表存储目录为{}",hiveTablePath);

        // 先判断对应的HDFS目录是否存在，不存在先创建
        String partitionDir = hiveTablePath.concat("/").concat("date=" + date).concat("/");
        if(!fs.exists(new Path(partitionDir))){
            logger.info("创建HDFS目录{}",partitionDir);
            fs.mkdirs(new Path(partitionDir));
        } else {
            logger.info("HDFS目录{}已存在",partitionDir);
        }

        // 存储ORC文件
        saveOrcFile(fs, partitionDir.concat(String.valueOf(System.currentTimeMillis())).concat(".orc"), rows, standardStructObjectInspector);

        return true;
    }


}
