package com.mob.dpi.dm

import com.mob.dpi.helper.MySqlDpiStatHelper.DpiFeedBackStat
import com.mob.dpi.helper.StatHelper
import com.mob.dpi.traits.Cacheable
import com.mob.dpi.util.PropUtils
import com.mob.dpi.{JobContext, Params}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.control.Breaks.{break, breakable}

case class PlatDuid(jobContext: JobContext) extends Cacheable {

  val params: Params = jobContext.params

  val idType: String = if (params.modelType.equals("idfa")) "idfa" else "imei"
  val mappingPartition = s"id_type=$idType/source=${params.source}"
  val partMap = Map("load_day" -> params.day, "source" -> params.source, "model_type" -> params.modelType)

  @transient implicit val spark: SparkSession = jobContext.spark

  val helper = new StatHelper(spark)

  import helper._

  def run(): Unit = {

    val param = if (params.source.equalsIgnoreCase("hebei_mobile")
    || params.source.equalsIgnoreCase("sichuan_mobile")
    ) {
      Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_IMEI_DUID_MAPPING, "Duid", params.force)
    } else {
      Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_IMEI_DUID_MAPPING, "Duid", params.force)
    }
    doWithStatus(param, calculate)

    spark.stop()
  }

  def lastVersion(): String = {
    var _version = params.imDay

    if (StringUtils.isEmpty(_version)) {
      _version = spark.sql(
        s"show partitions ${PropUtils.HIVE_TABLE_DM_DPI_EXT_ID_MAPPING}"
      ).collect().map(_.getString(0)).filter(
        _.contains(mappingPartition)
      ).last.split("/").last.split("=").last
    }

    logger.info(s"last version => ${_version}")
    _version
  }

  def calculate(stat: DpiFeedBackStat): Unit = {

    def getOrEmpty(s: String) = {
      val arr = s.split("\\$")
      if (arr.length <= 1) {
        ""
      } else {
        arr(1)
      }
    }

    spark.udf.register("extract_device_info", udf((line: String) => {
      if (StringUtils.isBlank(line) || line.split(",").length != 3) {
        ("", "", "")
      } else {
        val Array(b, n, v) = line.split(",")
        (getOrEmpty(b), getOrEmpty(n), getOrEmpty(v))
      }
    }))

    spark.udf.register("duidDeCoder", udf((duid: String, src: String) => {

      if (src.equalsIgnoreCase("unicom") ||
        src.equalsIgnoreCase("henan_mobile")) {
        val sb = StringBuilder.newBuilder
        // 针对大写字母是否转换的状态
        var convert = true
        if (!StringUtils.isEmpty(duid)) {
          for (alp <- duid) {

            breakable {

              // 小写字母转数字
              // 0a,1b,2c,3d,4e,5f,6g,7h,8i,9j
              if (96 < alp && alp < 123) {
                sb.append((alp - 49).toChar)
                break
              }
              // 大写字母转小写字母
              if (64 < alp && alp < 91) {

                // 大写字母需要转小写字母
                if (convert) {
                  sb.append((alp + 32).toChar)
                } else {
                  // 跳过转换,并重置convert => true
                  sb.append(alp)
                  convert = true
                }
                break
              }
              // 其他字符不变
              sb.append(alp)
            }
          }
          sb.toString()
        } else {
          sb.toString()
        }
      } else {
        duid
      }
    }))


    if (stat.source.equalsIgnoreCase("henan_mobile")) {
      // 匹配value, idfa|imei
      sql(
        s"""
           |insert overwrite table ${stat.dstTableNameWithDB}
           |partition (load_day='${stat.loadDay}',source='${stat.source}',
           |model_type='${stat.modelType}',day='${stat.day}')
           |select   value_md5_14 ieid, pid, id_type,
           |         split(split(tag, '#')[1], '\\\\$$')[0] plat,
           |         duidDeCoder(split(split(tag, '#')[1], '\\\\$$')[1],t1.source) duid,
           |         '' brand,
           |         '' market_name,
           |         '' os_type,
           |         if (size(split(tag, '#')) > 2, duidDeCoder(split(tag, '#')[2],t1.source), '') ifid
           |from (
           |         select   * from ${stat.srcTableNameWithDB}
           |         where    tag rlike '#'
           |         and      (split(split(tag, '#')[1], '\\\\$$')[0] != '0' or
           |                    (size(split(tag, '#')) > 2 and split(tag, '#')[2] != ''))
           |         and      source='${stat.source}'
           |         and      load_day='${stat.loadDay}'
           |         and      model_type='${stat.modelType}'
           |         and      day='${stat.day}'
           |) t1
           |join     ${PropUtils.HIVE_TABLE_DM_DPI_EXT_ID_MAPPING} t2
           |on       lower(t1.id) = lower(t2.id)
           |and      id_type='$idType' and t2.source='${stat.source}'
           |and      version='${lastVersion()}'
       """.stripMargin
      )
    } else if (stat.source.equalsIgnoreCase("hebei_mobile")) {
      sql(
        s"""
           |insert overwrite table ${stat.dstTableNameWithDB}
           |partition (load_day='${stat.loadDay}',source='${stat.source}',
           |model_type='${stat.modelType}',day='${stat.day}')
           |select   value_md5_14 ieid, pid, id_type,
           |         split(split(tag, '#')[1], '\\\\$$')[0] plat,
           |         duidDeCoder(split(split(tag, '#')[1], '\\\\$$')[1],t1.source) duid,
           |         '' brand,
           |         '' market_name,
           |         '' os_type,
           |         if (size(split(tag, '#')) > 2, duidDeCoder(split(tag, '#')[2],t1.source), '') ifid
           |from (
           |         select * from (
           |          select
           |           split(get_json_object(data,'$$.data'),'\\\\|')[0] as id
           |           , split(get_json_object(data,'$$.data'),'\\\\|')[1] as tag
           |           , load_day
           |           , source
           |           , model_type
           |           , day
           |          from
           |            ${stat.srcTableNameWithDB}
           |          where
           |            load_day='${stat.loadDay}'
           |            and      source='${stat.source}'
           |            and      model_type='${stat.modelType}'
           |            and      day='${stat.day}'
           |         ) s
           |         where    tag rlike '#'
           |         and      (split(split(tag, '#')[1], '\\\\$$')[0] != '0' or
           |                    (size(split(tag, '#')) > 2 and split(tag, '#')[2] != ''))
           |) t1
           |join     ${PropUtils.HIVE_TABLE_DM_DPI_EXT_ID_MAPPING} t2
           |on       lower(t1.id) = lower(t2.id)
           |and      id_type='$idType' and t2.source='${stat.source}'
           |and      version='${lastVersion()}'
       """.stripMargin
      )
    } else {
      // 匹配value, idfa|imei
      sql(
        s"""
           |insert overwrite table ${stat.dstTableNameWithDB}
           |partition (load_day='${stat.loadDay}',source='${stat.source}',
           |model_type='${stat.modelType}',day='${stat.day}')
           |select   value_md5_14 ieid, pid, id_type,
           |         split(split(tag, '#')[1], '\\\\$$')[0] plat,
           |         duidDeCoder(split(split(tag, '#')[1], '\\\\$$')[1],t1.source) duid,
           |         if (size(split(tag, '#')) > 2, extract_device_info(split(tag, '#')[2])._1, '') brand,
           |         if (size(split(tag, '#')) > 2, extract_device_info(split(tag, '#')[2])._2, '') market_name,
           |         if (size(split(tag, '#')) > 2, extract_device_info(split(tag, '#')[2])._3, '') os_type,
           |         if (size(split(tag, '#')) > 3, duidDeCoder(split(tag, '#')[3],t1.source), '') ifid
           |from (
           |         select   * from ${stat.srcTableNameWithDB}
           |         where    tag rlike '#'
           |         and      (split(split(tag, '#')[1], '\\\\$$')[0] != '0' or
           |                   (size(split(tag, '#')) > 2 and split(tag, '#')[2] != '')
           |                   or (size(split(tag, '#')) > 3 and split(tag, '#')[3] != ''))
           |         and      source='${stat.source}'
           |         and      load_day='${stat.loadDay}'
           |         and      model_type='${stat.modelType}'
           |         and      day='${stat.day}'
           |) t1
           |join     ${PropUtils.HIVE_TABLE_DM_DPI_EXT_ID_MAPPING} t2
           |on       lower(t1.id) = lower(t2.id)
           |and      id_type='$idType' and t2.source='${stat.source}'
           |and      version='${lastVersion()}'
       """.stripMargin
      )
    }
  }
}
