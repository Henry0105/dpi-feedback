package com.mob.dpi.dm

import com.mob.dpi.aes.AesTool
import com.mob.dpi.traits.Cacheable
import com.mob.dpi.udf.ReturnImei15
import com.mob.dpi.util.PropUtils
import com.mob.dpi.{JobContext, Params}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

case class StatisticsResult(jobContext: JobContext) extends Cacheable {

  val params: Params = jobContext.params
  val partition: String =
    s"day=${params.day}/" +
      s"source=${params.source}/" +
      s"model_type=${params.modelType}"

  val partMap = Map("load_day" -> params.day, "source" -> params.source, "model_type" -> params.modelType)
  val idType: String = if (params.modelType.equals("idfa")) "idfa" else "imei"
  val mappingPartition = s"id_type=$idType/source=${params.source}"

  @transient implicit val spark: SparkSession = jobContext.spark

  def udf(): Unit = {

    spark.udf.register("recomputationImei", ReturnImei15.evaluate _)

    // 山东aes解密
    spark.udf.register("AES_DECRYPT", AesTool.decrypt64 _)

    // tag 去除引号
    spark.udf.register("trimQuotes", trimQuotes _)
  }

  def run(): Unit = {

    // udf 注册
    udf()

    calculate()

    spark.stop()
  }


  def calculate(): Unit = {
    /** 数据格式一：id|tag:score,tag:score
     * 数据格式二：id|tag:score
     * */
    // 展开tag
    sql(
      s"""
         |select
         |id,
         |trimQuotes(split(t.tmp_tag,':')[0]) as tag,
         |if(size(split(t.tmp_tag,':'))>1,split(t.tmp_tag,':')[1],'') as cnt,
         |tag_limit_version,
         |load_day,source,model_type,day
         |from (
         |  select id,tag,tag_limit_version,load_day,source,model_type,day
         |  from ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_STAT}
         |  where load_day='${params.day}'
         |    and source = '${params.source}'
         |    and model_type='${params.modelType}'
         |    group by id,tag,tag_limit_version,load_day,source,model_type,day
         |  ) s
         |lateral view explode(split(tag,',')) t tmp_tag
         |where tag is not null and tag != ''
         |""".stripMargin)
      .repartition(1)
      .createOrReplaceTempView("src_data_temp")

    // 入库
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""
         |insert overwrite table ${PropUtils.HIVE_TABLE_DPI_MKT_TAG_STATISTICS_RESULT}
         |partition (load_day='${params.day}',source='${params.source}',
         |model_type='${params.modelType}',day)
         |select trim(id) id
         |       ,trim(tag) tag
         |       ,cnt
         |       ,day
         |from src_data_temp
         |where tag IS NOT NULL
       """.stripMargin
    )

  }

  /**
   * 修复tag 带引号
   */
  def trimQuotes(rawTag: String): String = {
    if (StringUtils.isNoneEmpty(rawTag) && rawTag.contains("\"")) {
      rawTag.replace("\"", "")
    } else {
      rawTag
    }
  }
}
