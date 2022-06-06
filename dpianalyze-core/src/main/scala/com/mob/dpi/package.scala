package com.mob

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mob.dpi.util.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.immutable.StringLike

package object dpi {

  /**
   * 任务参数类
   *
   * @param day       计算日期, yyyyMMdd格式, 默认T-2
   * @param source    运营商, 支持: unicom
   * @param modelType id类型, 支持: idfa|game|common
   * @param jobs      任务执行列表, 1-MappingValue
   * @param local     是否本地模式
   * @param force     是否覆盖写入
   * @param imDay     id_mapping的Day分区版本
   */
  case class Params(day: String = LocalDate.now().
    plusDays(-2).format(DateTimeFormatter.ofPattern("yyyyMMdd")),
                    source: String = "unicom",
                    modelType: String = "game",
                    jobs: String = "1",
                    province: String = "all",
                    local: Boolean = false,
                    mapping: Boolean = false,
                    force: Boolean = false,
                    imDay: String = "",
                    startDay: String = "",
                    monthType: String = "false"
                   ) extends Serializable

  case class JobContext(params: Params) {

    @transient implicit val spark: SparkSession = {
      var _builder = SparkSession.builder()
      if (params.local) _builder = _builder.master("local[*]")
      _builder.enableHiveSupport().getOrCreate()
    }

    lazy val otherArgs =
      Map("startDay" -> params.startDay.trim, "monthType" -> params.monthType,
        "mapTabPre" -> "dpi_analysis_test.mappingTab_temp")
  }

  case class TagOfflineInfo(id: Int, carrierEn: String, carrierZh: String, tag: String,
                            effectiveDay: Timestamp, userEmail: String, checkTimes: Int)

  def formatStr(str: String, length: Int): String = {
    s"%-${length}s".format(str)
  }

  def f8(str: String): String = {
    formatStr(str, 8)
  }

  def f16(str: String): String = {
    formatStr(str, 16)
  }

  def f24(str: String): String = {
    formatStr(str, 24)
  }

  def updateNoneEmpty(target: Map[String, String], kv: (String, String)): Map[String, String] = {
    if (StringUtils.isNoneBlank(kv._2)) {
      target + kv
    } else {
      target
    }
  }
}
