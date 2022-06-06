package com.mob.dpi.dm

import com.mob.dpi.aes.AesTool
import com.mob.dpi.aes.AesToolV2
import com.mob.dpi.enums.SourceType
import com.mob.dpi.enums.SourceType._
import com.mob.dpi.traits.Cacheable
import com.mob.dpi.udf.ReturnImei15
import com.mob.dpi.helper.MySqlDpiStatHelper.DpiFeedBackStat
import com.mob.dpi.helper.StatHelper
import com.mob.dpi.util.PropUtils
import com.mob.dpi.{JobContext, Params}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

case class DeviceTagResult(jobContext: JobContext) extends Cacheable {
  println(s"dbgparam-1:$param")
  val params: Params = jobContext.params
  val partition: String =
    s"day=${params.day}/" +
      s"source=${params.source}/" +
      s"model_type=${params.modelType}"
  println(s"partition-1:$partition")


  val partMap = Map("load_day" -> params.day, "source" -> params.source, "model_type" -> params.modelType)
  val idType: String = if (params.modelType.equals("idfa")) "idfa" else "imei"
  val mappingPartition = s"id_type=$idType/source=${params.source}"

  @transient implicit val spark: SparkSession = jobContext.spark

  import spark.implicits._

  val helper = new StatHelper(spark)

  import helper._

  def udf(): Unit = {
    println(s"partition-2:$partition")

    spark.udf.register("recomputationImei", ReturnImei15.evaluate _)

    // 山东aes解密
    spark.udf.register("AES_DECRYPT", AesTool.decrypt64 _)

    // 山东aes加密
    spark.udf.register("AES_ENCRYPT", AesToolV2.encrypt64 _)

    // tag转换
    spark.udf.register("tagMapping", tagMapping _)

    // tag替换
    spark.udf.register("replace_tag", mappingNewTag _)

    // tag 去除引号
    spark.udf.register("trimQuotes", trimQuotes _)
  }

  def run(): Unit = {

    // udf 注册
    udf()
    println(s"dbgparam1:$param")
    doWithStatus(param, calculate)

    spark.stop()
  }

  def calculate(stat: DpiFeedBackStat): Unit = {
    import stat._
    println(s"stat = ${stat.toString}")

    // tag mapping 校验
    SourceType.withName(source) match {
      case SHANDONG => assert(_tagMapping.contains(day), s"[source=$source][day=$day] tag mapping is not exist")
      case _ =>
    }

    // 支持两类表结构 id | tag / id | tag | score
    // 文件格式: id | tag1:score1,tag2:score2 /
    // 江苏移动的源表是 ods_dpi_mkt_feedback_incr_js, 且score为新增字段, 单独处理

    val oriDF = jsonTable match {
      case "0" =>
        sql(
          s"""
             |select
             |lower(${idSqlFragment}) id
             |, ${tagValueMappingSqlFragment} tag
             |, ${tagLimitVersionFragment} as tag_limit_version
             |, load_day
             |, source
             |, model_type
             |, day
             |from $srcTableNameWithDB
             |where
             |load_day='$loadDay'
             |and source='${
            if (source.equals("guangdong_mobile_new")) {
              "guangdong_mobile"
            } else {
              source
            }
          }'
             |and model_type='$modelType'
             |and day='$day'
             |""".stripMargin).cache()
      case "1" =>
        sql(
          s"""
             |select
             |lower(${idSqlFragment}) id
             |, ${tagValueMappingSqlFragment} tag
             |, ${tagLimitVersionFragment} as tag_limit_version
             |, load_day
             |, source
             |, model_type
             |, day
             |from (
             |  select
             |  ${jsonId}
             |  , ${jsonTag}
             |  , load_day
             |  , source
             |  , model_type
             |  , day
             |  from $srcTableNameWithDB
             |  where
             |  load_day='$loadDay'
             |  and source='$source'
             |  and model_type='$modelType'
             |  and day='$day'
             |) s
             |""".stripMargin).cache()
    }


    val oriCnt = oriDF.count()
    println(s"oriCnt: ${oriCnt}")
    oriDF.show(50, false)
    oriDF.createOrReplaceTempView("origin_data")


    val mappingDF = spark.table(PropUtils.HIVE_TABLE_DM_DPI_EXT_ID_MAPPING)
      .where(s"id_type='${idType}' and source='${
        if (source.equals("guangdong_mobile_new")) {
          "guangdong_mobile"
        } else {
          source.replace("unicom_proxy", "unicom")
        }
      }' and version='${lastVersion()}'")
    if (oriCnt > 0 && oriCnt < 5000000) {
      import spark.sparkContext.broadcast
      val bf = broadcast(oriDF.stat.bloomFilter("id", oriCnt, 0.01))
      mappingDF
        .filter(r => {
          val idStr = r.getAs[String]("id")
          StringUtils.isNoneBlank(idStr) && bf.value.mightContainString(idStr.toLowerCase())
        })
        .createOrReplaceTempView("bloom_filtered_mapping_tb")
    } else {
      mappingDF
        .createOrReplaceTempView("bloom_filtered_mapping_tb")
    }


    // 匹配value, idfa|imei
    sql(
      s"""
         |select lower(t2.id) id, t1.tag, value_md5_14, pid, t1.day, t1.tag_limit_version,
         |value_md5_15 as ieid15,
         |plat,
         |pid_ltm,
         |province_cn,
         |carrier
         |from origin_data t1
         |join bloom_filtered_mapping_tb t2
         |on lower(trim(t1.id)) = lower(trim(t2.id))
       """.stripMargin
    ).cache().createOrReplaceTempView("tag_value_mapping_tmp")

    sql("select * from tag_value_mapping_tmp").show(50, false)


    // 拆分tag
    if (source.equals("unicom_proxy")) {
      println(s"dbgparam-1.1:$param")

      sql(
        s"""
           |select id
           |       ,trimQuotes(split(regexp_replace(tag_exp,'\\#',''),'\\:')[0]) as tag
           |       ,case when size(split(regexp_replace(tag_exp,'\\#',''),'\\:')) > 1
           |        then split(regexp_replace(tag_exp,'\\#',''),'\\:')[1] else 1 end as times
           |       ,case when size(split(regexp_replace(tag_exp,'\\#',''),'\\:')) > 1
           |        then split(regexp_replace(tag_exp,'\\#',''),'\\:')[1] else 1 end as merge_times
           |       ,value_md5_14
           |       ,pid
           |       ,day
           |       ,tag_limit_version,
           |        ieid15,
           |        plat,
           |        pid_ltm,
           |        province_cn,
           |        carrier
           |from tag_value_mapping_tmp
           |LATERAL VIEW explode(split(tag,'\\,')) t1 as tag_exp
       """.stripMargin
      ).repartition(1).cache().createOrReplaceTempView("tag_explode_tmp")
    } else {
      sql(
        s"""
           |select id
           |       ,trimQuotes(split(tag_exp,'\\:')[0]) as tag
           |       ,case when size(split(tag_exp,'\\:')) > 1 then split(tag_exp,'\\:')[1] else 1 end as times
           |       ,case when size(split(tag_exp,'\\:')) > 2 then split(tag_exp,'\\:')[2] else 1 end as merge_times
           |       ,value_md5_14
           |       ,pid
           |       ,day
           |       ,tag_limit_version,
           |        ieid15,
           |        plat,
           |        pid_ltm,
           |        province_cn,
           |        carrier
           |from tag_value_mapping_tmp
           |LATERAL VIEW explode(split(tag,'\\,')) t1 as tag_exp
       """.stripMargin
      ).repartition(1).cache().createOrReplaceTempView("tag_explode_tmp")

    }
    sql("select * from tag_explode_tmp").show(50, false)


    var preSinkTable: String = "tag_explode_tmp"

    // tag mapping
    if (params.mapping) {
      println(s"dbgparam-1.2:$param")

      sql(
        s"""
           |select id
           |       ,$tagSqlFragment tag
           |       ,times
           |       ,merge_times
           |       ,value_md5_14
           |       ,pid
           |       ,tag_limit_version,
           |        ieid15,
           |        plat,
           |        pid_ltm,
           |        province_cn,
           |        carrier
           |from tag_explode_tmp
           |""".stripMargin).createOrReplaceTempView("tag_mapping_tmp")

      preSinkTable = "tag_mapping_tmp"

    }

    val finalImeiIdfaSql =
      if (params.modelType.equals("idfa")) {
        s"""
           |'' as ieid
           |,value_md5_14 as ifid
         """.stripMargin
      } else {
        // case when value rlike '^[0-9]{14,15}$' and length(value)=14
        // then recomputationImei(value) else value end as
        """
          |value_md5_14 as ieid
          |,'' as ifid
          """.stripMargin
      }

    // 入库
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""
         |insert overwrite table $dstTableNameWithDB
         |partition (load_day='$loadDay',source='${
        if (source.equals("guangdong_mobile_new")) {
          "guangdong_mobile"
        } else {
          source
        }
      }',
         |model_type='$modelType', day='$day')
         |select trim(id) id
         |       ,trim(tag) tag
         |       ,times
         |       ,merge_times
         |       ,$finalImeiIdfaSql
         |       ,pid
         |       ,tag_limit_version,
         |        ieid15,
         |        plat,
         |        pid_ltm,
         |        province_cn,
         |        carrier
         |from $preSinkTable
         |where tag IS NOT NULL
       """.stripMargin
    )

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

  val newTagMapping: Option[Broadcast[Map[String, String]]] = {
    if (params.mapping && params.source.equalsIgnoreCase(SICHUAN.toString)) {
      assert(spark.sql(s"SHOW PARTITIONS ${PropUtils.HIVE_TABLE_DM_DPI_MKT_URL_TAG_COMMENT_EXTENAL}"
      ).map(_.getString(0)).collect().toSet.contains(s"source=${params.source}"),
        s"mapping source not exists, source[${params.source}]," +
          s"table[${PropUtils.HIVE_TABLE_DM_DPI_MKT_URL_TAG_COMMENT_EXTENAL}]")

      Some(spark.sparkContext.broadcast(
        spark.table(PropUtils.HIVE_TABLE_DM_DPI_MKT_URL_TAG_COMMENT_EXTENAL
        ).filter($"source" === params.source
        ).select("tag", "tag_new").collect().map(r => r.getString(1) -> r.getString(0)).toMap))

    } else None
  }

  def mappingNewTag(tag: String): String = {
    if (StringUtils.isBlank(tag)) {
      null
    } else {
      val arr = tag.split(":")
      if (arr.length <= 1) {
        newTagMapping.get.value.getOrElse(tag, null)
      } else {
        newTagMapping.get.value.getOrElse(arr.head, null) + ":" + arr.tail.mkString(":")
      }
    }
  }

  println(s"dbgparam-2:$param")

  /* --tag mapping code begin-- */

  // Type2: tag -> newTag
  var param: Param = _
  var tm: String = _
  var tagSqlFragment: String = _
  var tagValueMappingSqlFragment: String = _
  var idSqlFragment: String = _
  var tagLimitVersionFragment: String = _
  var tagMappingBC: Broadcast[mutable.Map[String, mutable.Map[String, String]]] = _
  var _tagMapping: mutable.Map[String, mutable.Map[String, String]] = mutable.Map.empty
  var jsonTable: String = _
  // json 表中 data 解析
  var jsonId: String = _
  var jsonTag: String = _

  SourceType.withName(params.source) match {
    case UNICOM_PROXY =>
      tm = ""
      tagSqlFragment = ""
      tagValueMappingSqlFragment = " tag "
      idSqlFragment = " trim(id) "
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case SHANDONG =>
      tm = PropUtils.HIVE_TABLE_DPI_MKT_TAG_MAPPING_SHANDONG
      tagSqlFragment = " tagMapping(tag, day) "
      tagValueMappingSqlFragment = " concat(tag, ':', score) "
      idSqlFragment = " AES_ENCRYPT(AES_DECRYPT(id)) "
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_SD, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case TELECOM =>
      // tag 映射表
      tm = ""
      // tag 转换
      tagSqlFragment = ""
      // tag 和 score分开时处理
      tagValueMappingSqlFragment = " concat(tag, ':', '1') "
      // join 前 id 处理
      idSqlFragment = " trim(id) "
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_TELECOM, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case TELECOM_TIMEWINDOW =>
      // tag 映射表
      tm = ""
      // tag 转换
      tagSqlFragment = ""
      // tag 和 score分开时处理
      tagValueMappingSqlFragment = " concat(tag, ':', '1') "
      // join 前 id 处理
      idSqlFragment = " trim(id) "
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_TELECOM, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case UNICOM =>
      tm = ""
      tagSqlFragment = ""
      tagValueMappingSqlFragment = " split(tag, '#')[0] "
      idSqlFragment = " id "
      tagLimitVersionFragment = " tag_limit_version "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case HENAN =>
      tm = ""
      tagSqlFragment = ""
      tagValueMappingSqlFragment = " split(tag, '#')[0] "
      idSqlFragment = " id "
      tagLimitVersionFragment = " tag_limit_version "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case HEBEI =>
      // tag 转换表
      tm = ""
      // tag转换函数
      tagSqlFragment = ""
      // 对原始tag处理
      tagValueMappingSqlFragment = " split(tag, '#')[0] "
      // 对id 处理
      idSqlFragment = " id "
      // taglimit的版本号
      tagLimitVersionFragment = " '' "
      // 每家运营商 json data 结构不一致,分别解析
      jsonId = s"split(get_json_object(data,'$$.data'),'\\\\|')[0] as id"
      jsonTag = s"split(get_json_object(data,'$$.data'),'\\\\|')[1] as tag"

      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "1"
    case SICHUAN =>
      // tag 转换表
      tm = ""
      // tag转换函数
      tagSqlFragment = ""
      // 对原始tag处理 手动添加score = 1
      tagValueMappingSqlFragment = " concat(tag, ':', '1') "
      // 对id 处理
      idSqlFragment = " id "
      // taglimit的版本号
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "1"
      // 每家运营商 json data 结构不一致,分别解析
      jsonId = s"split(get_json_object(data,'$$.data'),'\\\\|')[3] as id"
      jsonTag = s"split(get_json_object(data,'$$.data'),'\\\\|')[2] as tag"
    case GUANGDONG =>
      tm = ""
      tagSqlFragment = ""
      tagValueMappingSqlFragment = " concat(trim(split(split(data, '\\\\|')[1], '#')[0]), ':', trim(split(split(data, '\\\\|')[1],'#')[1])) "
      idSqlFragment = " trim(split(data, '\\\\|')[0]) "
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_GD, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
    case _ =>
      tm = ""
      tagSqlFragment = ""
      tagValueMappingSqlFragment = "  split(tag, '#')[0] "
      idSqlFragment = " id "
      tagLimitVersionFragment = " '' "
      param = Param(PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR, partMap,
        PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT, "Tag", params.force)
      jsonTable = "0"
  }
  println(s"dbgparam-3:$param")


  if (StringUtils.isNoneBlank(tm) && params.mapping) {
    println(s"dbgparam-4:$param")
    spark.sql(
      s"""
         |select tag, tag_new, day
         |from $tm
         |where
         |load_day = '${params.day}'
         |and source = '${params.source}'
         |and model_type = '${params.modelType}'
         |""".stripMargin)
      .dropDuplicates("tag", "tag_new", "day")
      .collect()
      .foreach(row => {
        val day = row.getAs[String]("day").trim
        val tmp = _tagMapping.getOrElse(day, mutable.Map.empty[String, String])
        tmp.put(row.getAs("tag_new"), row.getAs("tag"))
        _tagMapping.put(day, tmp)
      })

    tagMappingBC = spark.sparkContext.broadcast(_tagMapping)
  }

  def tagMapping(tag: String, day: String): String = {
    println(s"dbgparam-5:$param")

    if (StringUtils.isBlank(tag) || StringUtils.isBlank(day)) {
      ""
    } else {
      tagMappingBC.value.get(day) match {
        case Some(tagMap) => tagMap.getOrElse(tag, "")
        case None => ""
      }
    }
  }

  /* --tag mapping code end-- */

  /**
   * 修复tag 带引号
   */
  def trimQuotes(rawTag: String): String = {
    println(s"dbgparam-6:$param")

    if (StringUtils.isNoneEmpty(rawTag) && rawTag.contains("\"")) {
      rawTag.replace("\"", "")
    } else {
      rawTag
    }
  }
}
