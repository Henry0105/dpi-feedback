package com.mob.dpi.util

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object ApplicationUtils {
  private[this] lazy val logger: Logger = Logger.getLogger(PropUtils.getClass)

  private[this] lazy val prop: Properties = {
    val _prop = new Properties()
    val propFile = "application.properties"
    var propIn: InputStreamReader = null
    Try {
      val _propFile = s"${System.getenv("DPIANALYZE_HOME")}/conf/$propFile"
      propIn = if (new File(_propFile).exists()) {
        new InputStreamReader(new FileInputStream(_propFile))
      } else new InputStreamReader(PropUtils.getClass.getClassLoader.getResourceAsStream(propFile), "UTF-8")
      _prop.load(propIn)
      propIn.close()
    } match {
      case Success(_) =>
        logger.info(s"props loaded succeed from [$propFile], {${_prop}}")
        _prop
      case Failure(ex) =>
        if (propIn != null) propIn.close()
        logger.error(ex.getMessage, ex)
        throw new InterruptedException(ex.getMessage)
    }
  }

  private[this] def getProperty(key: String): String = prop.getProperty(key)
  lazy val MAIL_ADDRESS: String = getProperty("mail.address")
  lazy val JDBC_MYSQL_URL: String = getProperty("jdbc.mysql.url")
  lazy val JDBC_MYSQL_USERNAME: String = getProperty("jdbc.mysql.username")
  lazy val JDBC_MYSQL_PASSWORD: String = getProperty("jdbc.mysql.password")
  lazy val JDBC_MYSQL_TABLE_NAME: String = getProperty("jdbc.mysql.table.name")
  lazy val JDBC_MYSQL_DB_NAME: String = getProperty("jdbc.mysql.db.name")
  lazy val JDBC_MYSQL_DRIVER: String = "com.mysql.jdbc.Driver"

  // 2th mysql addr
  lazy val JDBC_MYSQL_URL2: String = getProperty("jdbc.mysql.url2")
  lazy val JDBC_MYSQL_USERNAME2: String = getProperty("jdbc.mysql.username2")
  lazy val JDBC_MYSQL_PASSWORD2: String = getProperty("jdbc.mysql.password2")
  lazy val JDBC_MYSQL_TABLE_NAME2: String = getProperty("jdbc.mysql.table.name2")
  lazy val JDBC_MYSQL_DB_NAME2: String = getProperty("jdbc.mysql.db.name2")

  lazy val RESULT_TABLES_MAIL_ADDRESS: String = getProperty("result.tables.mail.address")
  lazy val RES_BACKOFF_HOUR: Long = getProperty("res.backoff.hour").toLong
  lazy val SRC_BACKOFF_HOUR: Long = getProperty("src.backoff.hour").toLong
  lazy val MYSQL_DATA_REMAIN_HOUR: Long = getProperty("mysql.data.remain.hour").toLong
  lazy val QUERY_DATA_INTERVAL_DAY: Int = getProperty("query.data.interval.day").toInt


  lazy val DPI_COST_JDBC_MYSQL_HOST: String = getProperty("dpi.cost.jdbc.mysql.host")
  lazy val DPI_COST_JDBC_MYSQL_PORT: String = getProperty("dpi.cost.jdbc.mysql.port")
  lazy val DPI_COST_JDBC_MYSQL_USERNAME: String = getProperty("dpi.cost.jdbc.mysql.username")
  lazy val DPI_COST_JDBC_MYSQL_PASSWORD: String = getProperty("dpi.cost.jdbc.mysql.password")
  lazy val DPI_COST_JDBC_MYSQL_DB: String = getProperty("dpi.cost.jdbc.mysql.db")
  // 各运营商 数据时间
//  lazy val SD_DATA_OFFSET_DAY: Int = getProperty("sd.data.offset.day").toInt
//  lazy val HN_DATA_OFFSET_DAY: Int = getProperty("hn.data.offset.day").toInt
//  lazy val ZJ_DATA_OFFSET_DAY: Int = getProperty("zj.data.offset.day").toInt
//  lazy val TJ_DATA_OFFSET_DAY: Int = getProperty("tj.data.offset.day").toInt
//  lazy val GD_DATA_OFFSET_DAY: Int = getProperty("gd.data.offset.day").toInt
//  lazy val AH_DATA_OFFSET_DAY: Int = getProperty("ah.data.offset.day").toInt
//  lazy val JS_DATA_OFFSET_DAY: Int = getProperty("js.data.offset.day").toInt
//  lazy val UNICOM_DATA_OFFSET_DAY: Int = getProperty("unicom.data.offset.day").toInt
//  lazy val SC_DATA_OFFSET_DAY: Int = getProperty("sc.data.offset.day").toInt

  // 运营商延迟n小时告警
//  lazy val SD_WARNING_DELAY_HOUR: Long = getProperty("sd.warning.delay.hour").toLong
//  lazy val HN_WARNING_DELAY_HOUR: Long = getProperty("hn.warning.delay.hour").toLong
//  lazy val GD_WARNING_DELAY_HOUR: Long = getProperty("gd.warning.delay.hour").toLong
//  lazy val AH_WARNING_DELAY_HOUR: Long = getProperty("ah.warning.delay.hour").toLong
//  lazy val JS_WARNING_DELAY_HOUR: Long = getProperty("js.warning.delay.hour").toLong
//  lazy val UNICOM_WARNING_DELAY_HOUR: Long = getProperty("unicom.warning.delay.hour").toLong
//  lazy val SC_WARNING_DELAY_HOUR: Long = getProperty("sc.warning.delay.hour").toLong
//  lazy val ZJ_WARNING_DELAY_HOUR: Long = getProperty("zj.warning.delay.hour").toLong
//  lazy val TJ_WARNING_DELAY_HOUR: Long = getProperty("tj.warning.delay.hour").toLong

  lazy val WARNING_CXT = getProperty("warning.cxt").split(",").map(x => {
    val arr = x.split(":")
    val delay = arr.takeRight(2)(0).toInt
    val offset = arr.takeRight(2)(1).toInt
    arr(0) -> Array(delay, offset)
  }).toMap


}
