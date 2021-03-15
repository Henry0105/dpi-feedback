package com.mob.dpi.monitor

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.sql.{Date, DriverManager}
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mob.dpi.traits.Cacheable
import com.mob.dpi.util.ApplicationUtils._
import com.mob.dpi.util.{ApplicationUtils, PropUtils}
import com.mob.mail.MailSender
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import com.mob.dpi._


/**
 * 数据监控, 包括:
 * 1. rp_dpi_app_test.rp_dpi_mkt_device_tag_result
 *    ① 每运营商/模型的数据总量
 *    ② 每运营商/模型的count distinct(imei)总量
 *    ③ 每运营商/模型的标签总量
 *    ④ 每运营商/模型/标签的数据总量
 * 2. dw_dpi_feedback.ods_dpi_mkt_feedback_incr
 *    ① 每运营商/模型的count distinct(id)总量
 *
 * 监控方式:
 * ① 每天8:00, 22:00发送邮件通知当天的数据更新情况
 * ② 写入mysql, 使用grafana进行可视化, 通过grafana的alert manager进行报警
 */
case class DPIDataMonitor(jobContext: JobContext) extends Cacheable {

  @transient implicit val spark: SparkSession = jobContext.spark
  implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  import spark._
  import spark.implicits._

  implicit object DPIDataMonitorResOrdering extends Ordering[DPIDataMonitorRes] {
    override def compare(p1: DPIDataMonitorRes, p2: DPIDataMonitorRes): Int = {
      p1.load_day == p2.load_day match {
        case true =>
          p1.source == p2.source match {
            case true =>
              p1.model_type == p2.model_type match {
                case true =>
                  p1.day == p2.day match {
                    case true => p2.tag.compareTo(p1.tag)
                    case _ => p1.day.compareTo(p2.day)
                  }
                case _ => p1.model_type.compareTo(p2.model_type)
              }
            case _ => p1.source.compareTo(p2.source)
          }
        case _ => p1.load_day.compareTo(p2.load_day)
      }
    }
  }

  def run(): Unit = {

    val day = jobContext.params.day

    val tagResult = table(PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT).filter($"load_day" === day).cache()
    tagResult.createOrReplaceTempView("tag_result_temp")
    // 运营商:全局数量统计 id , imei(去重) , tag
    val totalCnt = sql(
      s"""
         |select   load_day, source, model_type, day, count(id) id_cnt, count(distinct imei) id_dis_cnt, count(distinct tag) tag_cnt
         |from     tag_result_temp
         |group by load_day, source, model_type, day
       """.stripMargin
    ).createOrReplaceTempView("total_cnt_temp")

    // 运营商: tag 维度统计 id
    val tagCnt = sql(
      s"""
         |select   load_day, source, model_type, day, tag, count(id) id_cnt_by_tag
         |from     tag_result_temp
         |group by load_day, source, model_type, day, tag
       """.stripMargin
    ).createOrReplaceTempView("tag_cnt_temp")

    // 运营商: incr表统计 id(去重)
    val idCnt = sql(
      s"""
         |select   load_day, source, model_type, day, count(distinct id) id_dis_cnt_incr
         |from     ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}
         |where    load_day='$day'
         |group by load_day, source, model_type, day
       """.stripMargin
    ).union(
      sql(
        s"""
           |select   load_day, source, model_type, day, count(distinct id) id_dis_cnt_incr
           |from     ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_SD}
           |where    load_day='$day'
           |group by load_day, source, model_type, day
       """.stripMargin
      )
    ).union(
      sql(
        s"""
           |select   load_day, source, model_type, day, count(distinct id) id_dis_cnt_incr
           |from     ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_TELECOM}
           |where    load_day='$day'
           |group by load_day, source, model_type, day
       """.stripMargin
      )
    ).createOrReplaceTempView("id_cnt_incr_temp")

    val res = sql(
      s"""
         |select   m1.load_day, m1.source, m1.model_type, m1.day, m1.tag, m1.tag_cnt, m1.id_cnt_by_tag, m1.id_cnt, m1.id_dis_cnt, m2.id_dis_cnt_incr
         |from     (
         |            select   t1.load_day, t1.source, t1.model_type, t1.day, t2.tag, t1.id_cnt, t1.id_dis_cnt, t1.tag_cnt, t2.id_cnt_by_tag
         |            from     total_cnt_temp t1
         |            join     tag_cnt_temp t2
         |            on       t1.load_day=t2.load_day
         |            and      t1.source=t2.source
         |            and      t1.model_type=t2.model_type
         |            and      t1.day=t2.day
         |) m1
         |join     id_cnt_incr_temp m2
         |on       m1.load_day=m2.load_day
         |and      m1.source=m2.source
         |and      m1.model_type=m2.model_type
         |and      m1.day=m2.day
       """.stripMargin
    ).as[DPIDataMonitorRes].cache()

    // HDFS输出下载到本地 -> 作为邮件附件发送, 可能存在编码问题, 跳过
    //res.repartition(1).sortWithinPartitions(
    //                   "load_day", "source", "model_type", "day", "tag"
    //                 ).write.mode(SaveMode.Overwrite).option("header", value = true).csv(s"dpi_data_monitor/$day")
    //val srcPath = fs.listStatus(
    //                 new Path(s"dpi_data_monitor/$day")
    //               ).filter(_.getPath.getName.endsWith("csv")).head.getPath
    //fs.copyToLocalFile(false, srcPath, new Path(localPath))

    val localRes: Array[DPIDataMonitorRes] = res.collect().sorted
    val localPath = s"/home/dpi_test/dpianalyze/dist/output/dpi_data_monitor/$day/dpi_data_monitor_$day.csv"
    val targetFile = new File(localPath)
    val targetDir = new File(targetFile.getParent)
    if (targetFile.exists()) {
      targetFile.delete()
    }
    if (!targetDir.exists()) {
      targetDir.mkdirs()
    }
    using(new PrintWriter(new OutputStreamWriter(new FileOutputStream(localPath)), true)) {
      out =>
        val header = "load_day,source,model_type,day,tag,tag_cnt,id_cnt_by_tag,id_cnt,id_dis_cnt,id_dis_cnt_incr"
        out.println(header)
        localRes.map(_.toCsvStr).foreach(out.println)
    }

    println(toStdoutStr(localRes))

    MailSender.sendMail(s"DPI Data Monitor[load_day=$day]",
                        s"${toHtmlStr(localRes)}",
                        ApplicationUtils.MAIL_ADDRESS,
                        "", localPath)

    // TODO 删除本地csv
    //if (File(scala.reflect.io.Path(localPath)).exists) {
    //  File(scala.reflect.io.Path(localPath)).delete()
    //}

    // 插入MySQL表, 用于可视化展示
    batchInsert(localRes, day)

    spark.stop()
  }

  def toHtmlStr(data: Array[DPIDataMonitorRes]): String = {
    s"""
       |<table border="1">
       |<tr>
       |<th>load_day</th>
       |<th>source</th>
       |<th>model_type</th>
       |<th>day</th>
       |<th>tag</th>
       |<th>tag_cnt</th>
       |<th>id_cnt_by_tag</th>
       |<th>id_cnt</th>
       |<th>id_dis_cnt</th>
       |<th>id_dis_cnt_incr</th>
       |${data.map(_.toHtmlString).mkString("")}
       |</table>
     """.stripMargin
  }

  def toStdoutStr(data: Array[DPIDataMonitorRes]): String = {
    s"|${f8("load_day")}|${f24("source")}|${f16("model_type")}|${f8("day")}|${f8("tag")}|" +
      s"${f8("tag_cnt")}|${f16("id_cnt_by_tag")}|${f16("id_cnt")}|" +
      s"${f16("id_dis_cnt")}|${f16("id_dis_cnt_incr")}|\n" +
      s"${data.mkString("\n")}"
  }

  // scalastyle:off
  def using[Closable <: {def close(): Unit}, T](conn: Closable)(f: Closable => T): T =
    try { f(conn) } finally { conn.close() }

  def batchInsert(data: Array[DPIDataMonitorRes], day: String): Unit = {
    var cnt = 0
    Class.forName(JDBC_MYSQL_DRIVER)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    using(DriverManager.getConnection(JDBC_MYSQL_URL, JDBC_MYSQL_USERNAME, JDBC_MYSQL_PASSWORD)) {
      conn =>
        using(conn.prepareStatement(
          s"""
             |INSERT INTO $JDBC_MYSQL_TABLE_NAME(
             |   load_day, source, model_type, day, tag, tag_cnt, id_cnt_by_tag, id_cnt, id_dis_cnt, id_dis_cnt_incr)
             |   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
             |   tag_cnt=VALUES(tag_cnt), id_cnt_by_tag=VALUES(id_cnt_by_tag), id_cnt=VALUES(id_cnt),
             |   id_dis_cnt=VALUES(id_dis_cnt), id_dis_cnt_incr=VALUES(id_dis_cnt_incr)
       """.stripMargin)) {
          pstat =>
            data.foreach(obj => {
              pstat.setDate(1, new Date(sdf.parse(obj.load_day).getTime))
              pstat.setString(2, obj.source)
              pstat.setString(3, obj.model_type)
              pstat.setDate(4, new Date(sdf.parse(obj.day).getTime))
              pstat.setString(5, obj.tag)
              pstat.setLong(6, obj.tag_cnt)
              pstat.setLong(7, obj.id_cnt_by_tag)
              pstat.setLong(8, obj.id_cnt)
              pstat.setLong(9, obj.id_dis_cnt)
              pstat.setLong(10, obj.id_dis_cnt_incr)
              pstat.addBatch
            })
            cnt = pstat.executeBatch.sum
        }
    }
    println(s"数据插入/更新成功, 表[$JDBC_MYSQL_DB_NAME.$JDBC_MYSQL_TABLE_NAME], load_day[$day], 数量[$cnt]")
  }
}


case class DPIDataMonitorRes(load_day: String, source: String, model_type: String, day: String,
  tag: String, tag_cnt: Long, id_cnt_by_tag: Long, id_cnt: Long, id_dis_cnt: Long, id_dis_cnt_incr: Long) {
  override def toString: String =
    s"|${f8(load_day)}|${f24(source)}|${f16(model_type)}|${f8(day)}|${f8(tag)}|${f8(tag_cnt.toString)}|" +
      s"${f16(id_cnt_by_tag.toString)}|${f16(id_cnt.toString)}|" +
      s"${f16(id_dis_cnt.toString)}|${f16(id_dis_cnt_incr.toString)}|"

  def toHtmlString: String =
    s"""
       |<tr>
       |<td>$load_day</td>
       |<td>$source</td>
       |<td>$model_type</td>
       |<td>$day</td>
       |<td>$tag</td>
       |<td>$tag_cnt</td>
       |<td>$id_cnt_by_tag</td>
       |<td>$id_cnt</td>
       |<td>$id_dis_cnt</td>
       |<td>$id_dis_cnt_incr</td>
       |</tr>
     """.stripMargin

  def toCsvStr: String =
    s"$load_day,$source,$model_type,$day,$tag,$tag_cnt,$id_cnt_by_tag,$id_cnt,$id_dis_cnt,$id_dis_cnt_incr"
}

/**
CREATE TABLE `t_dpi_data_statistics` (
  `id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `load_day` datetime NOT NULL COMMENT '数据加载日期',
  `source` varchar(24) NOT NULL COMMENT '数据提供方, 运营商',
  `model_type` varchar(16) NOT NULL COMMENT '血缘中target表（字段）的id',
  `day` datetime NOT NULL COMMENT '数据计算日期',
  `tag` varchar(16) NOT NULL COMMENT '标签id',
  `tag_cnt` int(11) NOT NULL COMMENT 'rp_dpi_app_test.rp_dpi_mkt_device_tag_result表标签数量',
  `id_cnt_by_tag` int(11) NOT NULL COMMENT 'rp_dpi_app_test.rp_dpi_mkt_device_tag_result表每个标签的id数量',
  `id_cnt` int(11) NOT NULL COMMENT 'rp_dpi_app_test.rp_dpi_mkt_device_tag_result表id总数',
  `id_dis_cnt` int(11) NOT NULL COMMENT 'rp_dpi_app_test.rp_dpi_mkt_device_tag_result表id去重数量',
  `id_dis_cnt_incr` int(11) NOT NULL COMMENT 'dw_dpi_feedback.ods_dpi_mkt_feedback_incr表id去重数量',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`load_day`, `source`, `model_type`, `day`, `tag`) USING BTREE,
  KEY `source_model_day_tag` (`source`,`model_type`,`day`, `tag`) USING BTREE,
  KEY `load_day` (`load_day`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8
 */