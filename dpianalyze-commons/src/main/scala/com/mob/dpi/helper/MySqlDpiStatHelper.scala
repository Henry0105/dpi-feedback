package com.mob.dpi.helper

import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat

import com.mob.dpi.util.ApplicationUtils._

import scala.collection.mutable.ArrayBuffer

object MySqlDpiStatHelper {

  val sdf_ = new SimpleDateFormat("yyyyMMdd")

  case class DpiFeedBackStat(var srcDB: String = "", var srcTable: String = "", var loadDay: String = "",
                             var source: String = "", var modelType: String = "",
                             var day: String = "", var feedbackType: String = "", var calculated: Int = 0,
                             var calFirstTime: Timestamp = null, var calLastTime: Timestamp = null,
                             var calCount: Long = 0, var reEntry: Long = 0, var warnMailLastTime: Timestamp = null,
                             var succeeded: Int = 0, var dstDB: String = "",
                             var dstTable: String = "", var rowType: Int = 1,
                             var srcDiscoverTime: Timestamp = null, var statVersion: Timestamp = null,
                             var createTime : Timestamp = null, var updateTime : Timestamp = null) {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def srcTableNameWithDB: String = {
      srcDB.concat(".").concat(srcTable)
    }
    def dstTableNameWithDB: String = {
      dstDB.concat(".").concat(dstTable)
    }

    def parseSucceeded: String = {
      succeeded match {
        case 0 => "FAIL"
        case 1 => "SUCCESS"
      }
    }
    def toWarnHtmlString(): String = {

      s"""
         |<tr>
         |<td>$loadDay</td>
         |<td>$source</td>
         |<td>$modelType</td>
         |<td>$day</td>
         |<td>$fmtStatus</td>
         |<td>$tableShow</td>
         |<td>${fmtDay(calLastTime)}</td>
         |<td>${fmtDay(warnMailLastTime)}</td>
         |</tr>
     """.stripMargin
    }

    def fmtDay(timestamp: Timestamp): String = {
      if (null != timestamp) {
        sdf.format(timestamp)
      } else {
        ""
      }
    }

    def fmtStatus(): String = {
      if (succeeded == 0) {
        s"<font color='#FF0000'>FAIL</font>".stripMargin
      } else {
        "SUCCESS"
      }
    }

    def tableShow(): String = {
      if (rowType == 0) {
        srcTable
      } else {
        dstTable
      }
    }
  }


  // scalastyle:off
  def using[Closable <: {def close(): Unit}, T](conn: Closable)(f: Closable => T): T =
    try { f(conn) } finally { conn.close() }

  def batchInsert(data: DpiFeedBackStat): Unit = {
    batchInsert(Array(data))
  }

  def batchInsert(data: Array[DpiFeedBackStat]): Unit = {
    var cnt = 0
    Class.forName(JDBC_MYSQL_DRIVER)
    using(DriverManager.getConnection(JDBC_MYSQL_URL2, JDBC_MYSQL_USERNAME2, JDBC_MYSQL_PASSWORD2)) {
      conn =>
        using(conn.prepareStatement(
          s"""
             |INSERT INTO $JDBC_MYSQL_TABLE_NAME2(
             |   src_db, src_table, load_day, source, model_type, day, feedback_type, calculated, cal_first_time,
             |   cal_last_time, cal_count, reentry, warn_mail_last_time, succeeded, dst_db, dst_table, row_type,
             |   src_discover_time, stat_version)
             |   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
             |   stat_version=VALUES(stat_version)
       """.stripMargin)) {
          ps =>
            data.foreach(obj => {
              ps.setString(1, obj.srcDB)
              ps.setString(2, obj.srcTable)
              ps.setString(3, obj.loadDay)
              ps.setString(4, obj.source)
              ps.setString(5, obj.modelType)
              ps.setString(6, obj.day)
              ps.setString(7, obj.feedbackType)
              ps.setInt(8, obj.calculated)
              ps.setTimestamp(9, obj.calFirstTime)
              ps.setTimestamp(10, obj.calLastTime)
              ps.setLong(11, obj.calCount)
              ps.setLong(12, obj.reEntry)
              ps.setTimestamp(13, obj.warnMailLastTime)
              ps.setInt(14, obj.succeeded)
              ps.setString(15, obj.dstDB)
              ps.setString(16, obj.dstTable)
              ps.setInt(17, obj.rowType)
              ps.setTimestamp(18, obj.srcDiscoverTime)
              ps.setTimestamp(19, obj.statVersion)
              ps.addBatch()
            })
            cnt = ps.executeBatch.sum
        }
    }
  }

  def update(data: DpiFeedBackStat): Int = {
    update(Array(data))
  }

  def update(datas: Array[DpiFeedBackStat]): Int = {
    var cnt = 0
    Class.forName(JDBC_MYSQL_DRIVER)
    using(DriverManager.getConnection(JDBC_MYSQL_URL2, JDBC_MYSQL_USERNAME2, JDBC_MYSQL_PASSWORD2)) {
      conn =>
        using(conn.prepareStatement(
          s"""
             |UPDATE $JDBC_MYSQL_TABLE_NAME2 SET
             |   calculated = ?,
             |   cal_first_time = ?,
             |   cal_last_time = ?,
             |   cal_count = ?,
             |   reentry = ?,
             |   warn_mail_last_time = ?,
             |   succeeded = ?,
             |   src_discover_time = ?,
             |   stat_version = ?
             |   WHERE src_db = ?
             |   AND src_table = ?
             |   AND load_day = ?
             |   AND source = ?
             |   AND model_type = ?
             |   AND day = ?
             |   AND dst_db = ?
             |   AND dst_table = ?
             |   AND row_type = ?
       """.stripMargin)) {
          ps =>
            datas.foreach(data => {
              ps.setInt(1, data.calculated)
              ps.setTimestamp(2, data.calFirstTime)
              ps.setTimestamp(3, data.calLastTime)
              ps.setLong(4, data.calCount)
              ps.setLong(5, data.reEntry)
              ps.setTimestamp(6, data.warnMailLastTime)
              ps.setInt(7, data.succeeded)
              ps.setTimestamp(8, data.srcDiscoverTime)
              ps.setTimestamp(9, data.statVersion)
              ps.setString(10, data.srcDB)
              ps.setString(11, data.srcTable)
              ps.setString(12, data.loadDay)
              ps.setString(13, data.source)
              ps.setString(14, data.modelType)
              ps.setString(15, data.day)
              ps.setString(16, data.dstDB)
              ps.setString(17, data.dstTable)
              ps.setInt(18, data.rowType)
              ps.addBatch()
            })
            cnt = ps.executeBatch().sum
        }
    }
    cnt
  }

  def updateSrc(data: DpiFeedBackStat): Int = {
    var cnt = 0
    Class.forName(JDBC_MYSQL_DRIVER)
    using(DriverManager.getConnection(JDBC_MYSQL_URL2, JDBC_MYSQL_USERNAME2, JDBC_MYSQL_PASSWORD2)) {
      conn =>
        using(conn.prepareStatement(
          s"""
             |UPDATE $JDBC_MYSQL_TABLE_NAME2 SET
             |   src_discover_time = ?
             |   WHERE src_db = ?
             |   AND src_table = ?
             |   AND load_day = ?
             |   AND source = ?
             |   AND model_type = ?
             |   AND row_type = ?
             |   AND src_discover_time IS NULL
       """.stripMargin)) {
          ps => {
            ps.setTimestamp(1, data.srcDiscoverTime)
            ps.setString(2, data.srcDB)
            ps.setString(3, data.srcTable)
            ps.setString(4, data.loadDay)
            ps.setString(5, data.source)
            ps.setString(6, data.modelType)
            ps.setInt(7, data.rowType)
            cnt = ps.executeUpdate()
          }
        }
    }
    cnt
  }

  def queryByLoadDay(loadDay: String, rowType: Int): ArrayBuffer[DpiFeedBackStat] = {
    val sql = s"SELECT * FROM $JDBC_MYSQL_TABLE_NAME2 WHERE load_day = ? AND row_type = ?"
    doQuery(sql, Array(loadDay, rowType), parseData)
  }

  def queryLoadDayWithInterval(loadDay: String, interval: Int, rowType: Int): ArrayBuffer[DpiFeedBackStat] = {
    val sql = s"SELECT * FROM $JDBC_MYSQL_TABLE_NAME2 WHERE load_day BETWEEN DATE_FORMAT(DATE_SUB(? ,INTERVAL ? DAY), '%Y%m%d') AND ? AND row_type = ?"
    doQuery(sql, Array(loadDay, interval, loadDay, rowType), parseData)
  }

  def getPartitionStats(query: DpiFeedBackStat):ArrayBuffer[DpiFeedBackStat]={
    val sql = s"""
                 |SELECT * FROM $JDBC_MYSQL_TABLE_NAME2
                 |   WHERE src_db = ?
                 |   AND src_table = ?
                 |   AND load_day = ?
                 |   AND source = ?
                 |   AND model_type = ?
                 |   AND row_type = ?
                 |   AND dst_db = ?
                 |   AND dst_table = ?
                 |""".stripMargin
    doQuery(sql, buildCondition(query), parseData)
  }

  def buildCondition(query: DpiFeedBackStat): Array[Any] = {
    Array(query.srcDB, query.srcTable, query.loadDay, query.source, query.modelType,
      query.rowType, query.dstDB, query.dstTable)
  }

  def getSrcStats(query: DpiFeedBackStat):ArrayBuffer[DpiFeedBackStat]={
    val sql = s"""
                 |SELECT * FROM $JDBC_MYSQL_TABLE_NAME2
                 |   WHERE src_db = ?
                 |   AND src_table = ?
                 |   AND load_day = ?
                 |   AND source = ?
                 |   AND model_type = ?
                 |   AND row_type = ?
                 |""".stripMargin
    doQuery(sql, buildSrcCondition(query), parseData)
  }

  def buildSrcCondition(query: DpiFeedBackStat): Array[Any] = {
    Array(query.srcDB, query.srcTable, query.loadDay, query.source, query.modelType, query.rowType)
  }

  def doQuery[R](sql: String, where: Array[Any], parser: ResultSet=> R): ArrayBuffer[R] = {
    var res = ArrayBuffer.empty[R]
    Class.forName(JDBC_MYSQL_DRIVER)
    using(DriverManager.getConnection(JDBC_MYSQL_URL2, JDBC_MYSQL_USERNAME2, JDBC_MYSQL_PASSWORD2)) {
      conn =>
        using(conn.prepareStatement(sql)) {
          ps => {
            for (index <- where.indices) {
              ps.setObject(index + 1, where(index))
            }
            using(ps.executeQuery()) {
              rs => {
                while (rs.next()) {
                  res += parser(rs)
                }
              }
            }
          }
        }
        res
    }
  }

  def deleteBeforeN(before : Timestamp): Int ={
    var cnt = 0
    Class.forName(JDBC_MYSQL_DRIVER)
    using(DriverManager.getConnection(JDBC_MYSQL_URL2, JDBC_MYSQL_USERNAME2, JDBC_MYSQL_PASSWORD2)) {
      conn =>
        using(conn.prepareStatement(
          s"""
             |DELETE FROM $JDBC_MYSQL_TABLE_NAME2
             |WHERE
             |create_time < ?
             |""".stripMargin)) {
          ps => {
            ps.setObject(1, before)
            cnt = ps.executeUpdate()
          }
        }
    }
    cnt
  }

  def parseData(rs : ResultSet): DpiFeedBackStat ={
    DpiFeedBackStat(
      rs.getString("src_db"),
      rs.getString("src_table"),
      rs.getString("load_day"),
      rs.getString("source"),
      rs.getString("model_type"),
      rs.getString("day"),
      rs.getString("feedback_type"),
      rs.getInt("calculated"),
      rs.getTimestamp("cal_first_time"),
      rs.getTimestamp("cal_last_time"),
      rs.getLong("cal_count"),
      rs.getLong("reentry"),
      rs.getTimestamp("warn_mail_last_time"),
      rs.getInt("succeeded"),
      rs.getString("dst_db"),
      rs.getString("dst_table"),
      rs.getInt("row_type"),
      rs.getTimestamp("src_discover_time"),
      rs.getTimestamp("stat_version"),
      rs.getTimestamp("create_time"),
      rs.getTimestamp("update_time")
    )
  }
}
