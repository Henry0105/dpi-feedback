package com.mob.dpi.beans

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mob.dpi.traits.Cacheable
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}


abstract class BaseCarrier(option: Option[SparkSession]) extends Cacheable {

  protected val comParam: ComParam

  private val local: Boolean = comParam.otherArgs.getOrElse("local", "false").toBoolean
  private val incrTab: String = comParam.otherArgs.getOrElse("incrTab", "")
  private val tagTab: String = comParam.otherArgs.getOrElse("tagTab", "")
  private val mappingTab1: String = comParam.otherArgs.getOrElse("mappingTab1", "")
  private val mappingTab2: String = comParam.otherArgs.getOrElse("mappingTab2", "")
  private val carrier: String = comParam.source
  private val endDay: String = comParam.loadDay
  private val startDay: String = {
    LocalDate.parse(endDay, DateTimeFormatter.ofPattern("yyyyMMdd"))
      .plusDays(-7)
      .format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  }


  @transient override implicit val spark: SparkSession = option.getOrElse({
    var _builder = SparkSession.builder()
    if (local) _builder = _builder.master("local[*]")
    _builder.enableHiveSupport().getOrCreate()
  })


  def source(spark: SparkSession): DataFrame


  def paramsCheck(): Boolean = {
    StringUtils.isBlank(incrTab) || StringUtils.isBlank(tagTab) || StringUtils.isBlank(carrier) || StringUtils.isBlank(endDay)
  }

  // incr表数据源
  def incrSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW incrTab_temp as
       |select source, load_day, day, model_type, id
       |from ${incrTab}
       |where source = '${carrier}' and load_day > '${startDay}' and load_day <= '${endDay}'
       |""".stripMargin
  }

  // tag表数据源
  def tagSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW tagTab_temp as
       |select source, load_day, day, model_type, tag_limit_version, tag, id
       |from ${tagTab}
       |where source = '${carrier}' and load_day > '${startDay}' and load_day <= '${endDay}'
       |""".stripMargin
  }

  // 按运营商统计
  def carrierSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW carrierSide_temp as
       |select source, load_day, day, model_type, count(1) id_cnt, count(distinct id) dup_id_cnt
       |from incrTab_temp
       |group by source, load_day, day, model_type
       |""".stripMargin
  }

  // 按tag方式统计(业务,tag,id数量)
  def platSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW platSide_temp as
       |select source, load_day, day, model_type, plat, sum(tag_cnt) tag_cnt, sum(dup_tag_cnt) dup_tag_cnt
       |from
       |(
       |    select a.source, a.load_day, a.day, a.model_type, a.tag, b.plat, a.tag_cnt, a.dup_tag_cnt
       |    from
       |    (
       |      select source, load_day, day, model_type, tag_limit_version, tag, count(1) tag_cnt, count(distinct id) dup_tag_cnt
       |      from tagTab_temp
       |      group by source, load_day, day, model_type, tag_limit_version, tag
       |    )a
       |    join mappingTab_temp b
       |    on a.tag = b.tag
       |)a
       |group by source, load_day, day, model_type, plat
       |""".stripMargin
  }


  // 按行业统计
  def cateSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW cateSide_temp as
       |select source, load_day, day, model_type, plat, cate_l1, sum(tag_cnt) tag_cnt, sum(dup_tag_cnt) dup_tag_cnt
       |from
       |(
       |    select a.source, a.load_day, a.day, a.model_type, a.tag, b.cate_l1, b.plat, a.tag_cnt, a.dup_tag_cnt
       |    from
       |    (
       |      select source, load_day, day, model_type, tag, count(1) tag_cnt , count(distinct id) dup_tag_cnt
       |      from tagTab_temp
       |      group by source, load_day, day, model_type, tag_limit_version, tag
       |    ) a
       |    join mappingTab_temp b
       |    on a.tag = b.tag
       |)a
       |group by source, load_day, day, model_type, plat, cate_l1
       |""".stripMargin
  }

  // mapping 元数据表
  def mapTab: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW mappingTab_temp as
       |select cate_l1, tag, plat
       |from
       |(
       |    select cate_l1, tag, case when plat rlike '智能' and cate_l1 = '其他' then '智能增长线_智赋' when plat rlike '智能' and cate_l1 = '接码欺诈' then '智能增长线_智弈' else plat end as plat
       |    from
       |    (
       |        select case when cate_l1 rlike '保险' or cate_l1 in ('新闻资讯','科技服务','婚恋交友','医疗健康','航旅','军事应用','智能设备','运动','天气','证券财经','网赚') then '保险'
       |         when cate_l1 rlike '游戏' or cate_l1 in ('传奇','三国','仙侠') then '游戏'
       |         when cate_l1 rlike '教育' or cate_l1 in ('K12','k12') then '教育'
       |         when cate_l1 in ('支付','测试') then '信用卡'
       |         when cate_l1 in ('高企','空调地暖','跨境电商','') then '其他'
       |         when cate_l1 in ('家装','装修','家装-家博会','家装-婚博会') then '装修'
       |         when cate_l1 rlike '培训' then '培训'
       |         else cate_l1 end as cate_l1, tag,
       |        case when plat rlike '智赋' then '智能增长线_智赋'
       |         when plat rlike '智弈' then '智能增长线_智弈'
       |         when plat rlike '智能' then '智能增长线_智汇'
       |         when plat rlike '金融' then '金融线'
       |         when plat rlike '平台|di|DI' then '平台'
       |         else plat end as plat
       |        from ${mappingTab1}
       |    )a
       |    union all
       |    select cate_l1, tag, case when plat rlike '智能' and cate_l1 = '其他' then '智能增长线_智赋' when plat rlike '智能' and cate_l1 = '接码欺诈' then '智能增长线_智弈' else plat end as plat
       |    from
       |    (
       |        select case when cate_l1 rlike '保险' or cate_l1 in ('新闻资讯','科技服务','婚恋交友','医疗健康','航旅','军事应用','智能设备','运动','天气','证券财经','网赚') then '保险'
       |         when cate_l1 rlike '游戏' or cate_l1 in ('传奇','三国','仙侠') then '游戏'
       |         when cate_l1 rlike '教育' or cate_l1 in ('K12','k12') then '教育'
       |         when cate_l1 in ('支付','测试') then '信用卡'
       |         when cate_l1 in ('高企','空调地暖','跨境电商','') then '其他'
       |         when cate_l1 in ('家装','装修','家装-家博会','家装-婚博会') then '装修'
       |         when cate_l1 rlike '培训' then '培训'
       |         else cate_l1 end as cate_l1, tag,
       |        case when plat rlike '智赋' then '智能增长线_智赋'
       |        when plat rlike '智弈' then '智能增长线_智弈'
       |         when plat rlike '智能' then '智能增长线_智汇'
       |         when plat rlike '金融' then '金融线'
       |         when plat rlike '平台|di|DI' then '平台'
       |         else plat end as plat
       |        from ${mappingTab2}
       |    )a
       |)a
       |group by cate_l1, tag, plat
       |""".stripMargin
  }


  def prepare(): Unit = {
    sql(incrSrcSql)
    sql(tagSrcSql)
    sql(mapTab)
  }

  def process(): Unit = {

    if (paramsCheck) throw new Exception("params is error.")
    // 准备数据
    prepare()
    sql(carrierSideCost)
    sql(platSideCost)
    sql(cateSideCost)
  }
}

