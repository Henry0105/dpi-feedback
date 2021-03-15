package com.mob.dpi.monitor

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.mob.dpi.JobContext
import com.mob.dpi.enums.SourceType
import com.mob.dpi.enums.SourceType._
import com.mob.dpi.helper.MySqlDpiStatHelper
import com.mob.dpi.helper.MySqlDpiStatHelper.DpiFeedBackStat
import com.mob.dpi.util.{ApplicationUtils, DateUtils}
import com.mob.mail.MailSender

import scala.collection.mutable


/**
 * 数据监控, 包括:
 * 监控dpi:数据源表,结果表是否为空
 * 监控方式:
 * 手动运行
 * 邮件方式通知
 */
case class DPIMonitorFromMysqlMetric(jobContext: JobContext) {
  def run(): Unit = {

    val loadDay = jobContext.params.day
    println("LoadDay : " + loadDay)

    // 运营商回流的数据情况
    val backedDays: mutable.Map[String, mutable.Set[String]] = mutable.Map.empty
    // miss数据源
    val sSrc = MySqlDpiStatHelper.queryLoadDayWithInterval(loadDay, ApplicationUtils.QUERY_DATA_INTERVAL_DAY, 0)
    println("sSrc size : " + sSrc.length)

    // miss分区计算结果
    val pSrc = MySqlDpiStatHelper.queryLoadDayWithInterval(loadDay, ApplicationUtils.QUERY_DATA_INTERVAL_DAY, 1)
    println("pSrc size : " + pSrc.length)

    pSrc.foreach(r => {
      val tmp = backedDays.getOrElse(r.source, mutable.Set.empty[String])
      if (r.succeeded == 1) {
        tmp.add(r.day)
      }
      backedDays.put(r.source, tmp)
    })

    // 若数据夸天回来,过滤掉
    val sSrc2 = sSrc.filter(r => {

      val dataDay = DateUtils.preNDate(r.loadDay, "yyyyMMdd",
        ApplicationUtils.WARNING_CXT.getOrElse(r.source, Array.empty[Int])(1))

      val setTmp = backedDays.get(r.source)
      if (setTmp != null) {
        !setTmp.contains(dataDay)
      } else {
        false
      }
    }
    )

    val srcStats = sSrc2.filter(missSrcNeedMail).toArray
    println("SrcStats size : " + srcStats.length)

    val partitionStats = pSrc.filter(missResNeedMail).toArray
    println("partitionStats size : " + partitionStats.length)
    val mergeStats = srcStats ++ partitionStats

    if (mergeStats.nonEmpty) {
      // 邮件告警
      MailSender.sendMail(s"DPI Monitor[monitor_day=$loadDay]",
        s"${mergeMsg(srcStats, partitionStats)}",
        ApplicationUtils.RESULT_TABLES_MAIL_ADDRESS)
      MySqlDpiStatHelper.update(mergeStats)
    }

    jobContext.spark.stop()

    deleteDataN()
  }

  def mergeMsg(src: Array[DpiFeedBackStat], partition: Array[DpiFeedBackStat]): String = {
    "=>以下数据源不存在\n"
      .concat(toHtml(src, "Src_Table"))
      .concat("\n=>DB[rp_dpi_app_test]中,以下[Tag Result|Duid]表数据异常输出\n")
      .concat(toHtml(partition.filter(_.dstDB.equalsIgnoreCase("rp_dpi_app_test")), "Dst_Table"))
      .concat("\n=>DB[dm_dpi_master]中,以下[Tag Result|Duid]表数据异常输出\n")
      .concat(toHtml(partition.filter(_.dstDB.equalsIgnoreCase("dm_dpi_master")), "Dst_Table"))
  }

  def missSrcNeedMail(stat: DpiFeedBackStat): Boolean = {

    val delayHour = ApplicationUtils.WARNING_CXT.getOrElse(stat.source, Array.empty[Int])(0)

    if (stat.warnMailLastTime == null && stat.srcDiscoverTime == null &&
      backoffTime(stat.createTime, delayHour) ) {
      stat.warnMailLastTime = new Timestamp(new Date().getTime())
      true
    } else {
      false
    }
  }

  def missResNeedMail(stat: DpiFeedBackStat): Boolean = {
    if (stat.warnMailLastTime == null && stat.succeeded == 0
      && backoffTime(choiceBackoffTime(stat), ApplicationUtils.RES_BACKOFF_HOUR)) {
      stat.warnMailLastTime = new Timestamp(new Date().getTime())
      true
    } else {
      false
    }
  }

  def choiceBackoffTime(stat: DpiFeedBackStat): Timestamp = {
    if (stat.srcDiscoverTime == null) {
      stat.createTime
    } else {
      stat.srcDiscoverTime
    }
  }

  // 退避时间
  def backoffTime(timestamp: Timestamp, hour: Long): Boolean = {
    println(s"BackoffTime : $timestamp|$hour")
    new Date().getTime - timestamp.getTime  - hour*1000*3600 > 0
  }

  def maybeMail(stats: Array[DpiFeedBackStat]): Boolean = {
    stats.exists(st => {
      st.succeeded == 0 && st.warnMailLastTime == null
    })
  }

  def toHtml(msgs: Array[DpiFeedBackStat], tableHead: String): String = {
    s"""
       |<table border="1">
       |<tr>
       |<th>Load_Day</th>
       |<th>Source</th>
       |<th>Model_Type</th>
       |<th>Data_Day</th>
       |<th>Status</th>
       |<th>$tableHead</th>
       |<th>Cal_Last_Time</th>
       |<th>Warn_Mail_Last_Time</th>
       |</tr>
       |${msgs.map(_.toWarnHtmlString).mkString("")}
       |</table>
     """.stripMargin
  }

  def deleteDataN(): Unit = {
    val ts : Long = new Date().getTime-ApplicationUtils.MYSQL_DATA_REMAIN_HOUR*3600*1000
    println("DeleteDataN: " + MySqlDpiStatHelper.deleteBeforeN(new Timestamp(ts)))
  }
}