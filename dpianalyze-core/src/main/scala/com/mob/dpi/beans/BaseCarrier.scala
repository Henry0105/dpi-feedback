package com.mob.dpi.beans

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mob.dpi.traits.Cacheable
import com.mob.dpi.util.{JdbcTools, Jdbcs, PropUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


trait BaseCarrier extends Cacheable {


  protected val comParam: ComParam
  protected val sparkOpt: Option[SparkSession]

  protected val local: Boolean = comParam.otherArgs.getOrElse("local", "false").toBoolean
  protected val incrTab: String = comParam.otherArgs.getOrElse("incrTab", "")
  protected val tagTab: String = comParam.otherArgs.getOrElse("tagTab", "")
  protected val mappingTab1: String = comParam.otherArgs.getOrElse("mappingTab1", s"${PropUtils.HIVE_TABLE_DPI_MKT_URL_WITHTAG}")
  protected val mappingTab2: String = comParam.otherArgs.getOrElse("mappingTab2", s"${PropUtils.HIVE_TABLE_TMP_URL_OPERATORSTAG}")
  protected val carrier: String = comParam.source
  protected val endDay: String = comParam.loadDay
  protected val outOfModels = comParam.otherArgs.getOrElse("outOfModels", "")
  protected val startDay: String = comParam.otherArgs.getOrElse("startDay", {
    LocalDate.parse(endDay, DateTimeFormatter.ofPattern("yyyyMMdd"))
      .plusDays(-6)
      .format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  }
  )

  protected val monthType: Boolean = comParam.otherArgs.getOrElse("monthType", "false").toBoolean

  protected val mappingTabPre: String = comParam.otherArgs.getOrElse("mapTabPre", "")

  protected val month: String = endDay.trim.substring(0, 6)

  protected val toMysql: Boolean = comParam.otherArgs.getOrElse("toMysql", "true").toBoolean


  protected val calPrice: BigDecimal
  protected val dataPrice: BigDecimal

  @transient protected override implicit val spark: SparkSession = sparkOpt.getOrElse({
    var _builder = SparkSession.builder()
    if (local) _builder = _builder.master("local[*]")
    _builder.enableHiveSupport().getOrCreate()
  })


  protected def paramsCheck(): Boolean = {
    StringUtils.isBlank(incrTab) || StringUtils.isBlank(tagTab) || StringUtils.isBlank(carrier) || StringUtils.isBlank(endDay)
  }

  // incr表数据源
  protected def incrSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW incrTab_temp as
       |select source, load_day, day, model_type, id
       |from ${incrTab}
       |where source = '${carrier}' and load_day >= '${startDay}' and load_day <= '${endDay}'
       |and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |""".stripMargin
  }

  // tag表数据源
  protected def tagSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW tagTab_temp as
       |select source, load_day, day, model_type, tag_limit_version, tag, id
       |from ${tagTab}
       |where source = '${carrier}' and load_day >= '${startDay}' and load_day <= '${endDay}'
       |and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |""".stripMargin
  }

  // 按运营商统计 (若多模型设备重复,算多份钱)
  protected def carrierSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW carrierSide_temp as
       |select source, load_day, day
       |, id_cnt
       |, dup_id_cnt
       |, id_cnt cal_cnt
       |, round(id_cnt * ${dataPrice}, 4) carrier_cost
       |from
       |(
       |  select source, load_day, day, count(1) id_cnt, count(distinct id) dup_id_cnt from incrTab_temp
       |  group by source, load_day, day
       |)t
       |""".stripMargin
  }

  // 按tag方式统计(业务,tag,id数量)
  protected def platSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW platSide_temp as
       |select s.source, s.load_day, s.day, s.plat
       |, tag_cnt
       |, dup_tag_cnt
       |, round(t.plat_curr_sum/t.carrier_curr_sum, 4) plat_rate
       |, round(t.plat_curr_sum/t.carrier_curr_sum * ${calPrice}, 4) plat_cal_cost
       |, tag_cnt cal_cnt
       |, round(tag_cnt * ${dataPrice}, 4) plat_cost
       |, round(t.max_plat_curr_sum/t.max_carrier_curr_sum, 4) last_plat_rate
       |, round(t.max_plat_curr_sum/t.max_carrier_curr_sum * ${calPrice}, 4) last_plat_cal_cost
       |from
       |(
       |  select source, load_day, day, plat
       |  , sum(tag_cnt) tag_cnt
       |  , sum(dup_tag_cnt) dup_tag_cnt
       |  from
       |  (
       |      select a.source, a.load_day, a.day, a.tag, b.plat
       |      , a.tag_cnt
       |      , a.dup_tag_cnt
       |      from
       |      (
       |        select source, load_day, day, tag
       |        , count(1) tag_cnt
       |        , count(distinct id) dup_tag_cnt
       |        from tagTab_temp
       |        group by source, load_day, day, tag
       |      )a
       |      join mappingTab_temp b
       |      on a.tag = b.tag
       |  )a
       |  group by source, load_day, day, plat
       |)s join ${PropUtils.HIVE_TABLE_PLAT_DISTRIBUTION} t
       |on s.source = t.source and s.load_day = t.load_day and s.day = t.day and s.plat = t.plat
       |""".stripMargin
  }


  // 按行业统计
  protected def cateSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW cateSide_temp as
       |select source, load_day, day, plat, cate_l1, tag_cnt, dup_tag_cnt
       |, tag_cnt as cal_cnt
       |, round(tag_cnt * ${dataPrice} , 4) cate_l1_cost
       |from
       |(
       |  select source, load_day, day, plat, cate_l1
       |  , sum(tag_cnt) tag_cnt
       |  , sum(dup_tag_cnt) dup_tag_cnt
       |  from
       |  (
       |      select a.source, a.load_day, a.day, a.tag, b.cate_l1, b.plat
       |      , a.tag_cnt
       |      , a.dup_tag_cnt
       |      from
       |      (
       |        select source, load_day, day, tag
       |        , count(1) tag_cnt
       |        , count(distinct id) dup_tag_cnt
       |        from tagTab_temp
       |        group by source, load_day, day, tag
       |      ) a
       |      join mappingTab_temp b
       |      on a.tag = b.tag
       |  )a
       |  group by source, load_day, day, plat, cate_l1
       |)t
       |""".stripMargin
  }

  // mapping 元数据表
  protected def mapTab: String = {
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
       |         when plat rlike '智弈' then '智能增长线_智弈'
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

  // mapping 元数据表
  protected def mapTabPre: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW mappingTab_temp as
       |select cate_l1, tag, plat
       |from ${mappingTabPre}
       |""".stripMargin
  }

  // 每家运营商业务线分布占比 (数量与价格)
  protected def platDistribution: String = {
    s"""
       |insert overwrite table ${PropUtils.HIVE_TABLE_PLAT_DISTRIBUTION}
       |select source, load_day, day, plat, plat_tag_cnt, plat_curr_sum, carrier_curr_sum, max_plat_curr_sum, max_carrier_curr_sum
       |from
       |(
       |  select source, load_day, day, plat, plat_tag_cnt, plat_curr_sum, carrier_curr_sum
       |  , MAX (plat_curr_sum) OVER (PARTITION BY plat ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) max_plat_curr_sum
       |  , MAX (carrier_curr_sum) OVER (ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) max_carrier_curr_sum
       |  from
       |  (
       |    select source, load_day, day, plat
       |    , plat_tag_cnt
       |    ,SUM (plat_tag_cnt) OVER (PARTITION BY plat ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) plat_curr_sum
       |    ,SUM (plat_tag_cnt) OVER (ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) carrier_curr_sum
       |    from
       |    (
       |      select source, load_day, day, plat
       |      , sum(tag_cnt) plat_tag_cnt
       |      from
       |      (
       |        select source, load_day, day, tag
       |        , count(1) tag_cnt
       |        from ${tagTab}
       |        where
       |        source='${carrier}'
       |        and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |        and load_day >= date_format(to_timestamp(trunc(to_date('${endDay}', 'yyyyMMdd'), 'MM')), 'yyyyMMdd')
       |        and load_day <= '${endDay}'
       |        group by source, load_day, day, tag
       |      )a join mappingTab_temp b
       |      on a.tag = b.tag
       |      group by source, load_day, day, plat
       |    )t1
       |  )m1
       |  union all
       |  select source, load_day, day, plat, plat_tag_cnt, plat_curr_sum, carrier_curr_sum
       |  , MAX (plat_curr_sum) OVER (PARTITION BY plat ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) max_plat_curr_sum
       |  , MAX (carrier_curr_sum) OVER (ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) max_carrier_curr_sum
       |  from
       |  (
       |    select source, load_day, day, plat
       |    , plat_tag_cnt
       |    ,SUM (plat_tag_cnt) OVER (PARTITION BY plat ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) plat_curr_sum
       |    ,SUM (plat_tag_cnt) OVER (ORDER BY day asc, load_day asc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) carrier_curr_sum
       |    from
       |    (
       |      select source, load_day, day, plat
       |      , sum(tag_cnt) plat_tag_cnt
       |      from
       |      (
       |        select source, load_day, day, tag
       |        , count(1) tag_cnt
       |        from ${tagTab}
       |        where
       |        ${startDay} < date_format(to_timestamp(trunc(to_date('${endDay}', 'yyyyMMdd'), 'MM')), 'yyyyMMdd')
       |        and source='${carrier}'
       |        and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |        and load_day >= date_format(to_timestamp(trunc(to_date('${startDay}', 'yyyyMMdd'), 'MM')), 'yyyyMMdd')
       |        and load_day < date_format(to_timestamp(trunc(to_date('${endDay}', 'yyyyMMdd'), 'MM')), 'yyyyMMdd')
       |        group by source, load_day, day, tag
       |      )a join mappingTab_temp b
       |    on a.tag = b.tag
       |    group by source, load_day, day, plat
       |    )t2
       |  )m2
       |)t
       |where load_day >= '${startDay}' and load_day <= '${endDay}'
       |""".stripMargin
  }



  protected def prepare(): Unit = {
    sql(incrSrcSql)
    sql(tagSrcSql)

    if (tableExists(mappingTabPre)) {
      sql(mapTabPre)
    } else {
      sql(mapTab)
    }
    sql(platDistribution)

  }

  def process(): BaseCarrier = {

    if (paramsCheck) throw new Exception("params is error.")
    // 准备数据
    prepare()
    sql(carrierSideCost)
    sql(platSideCost)
    sql(cateSideCost)
    this
  }

  def insertIntoHive(): BaseCarrier = {
    if (!monthType) return this
    sql("set hive.exec.dynamic.partition=true")
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql(
      s"""insert overwrite table ${PropUtils.HIVE_TABLE_CARRIERSIDE_COST} partition(month, source)
         |select  load_day, day as data_day, id_cnt, dup_id_cnt
         |, cal_cnt, carrier_cost, '${month}' month, source
         |from carrierSide_temp""".stripMargin)
    sql(
      s"""insert overwrite table ${PropUtils.HIVE_TABLE_PLATSIDE_COST} partition(month, source)
         |select  load_day, day as data_day, plat, tag_cnt, dup_tag_cnt, plat_rate, plat_cal_cost
         |, cal_cnt, plat_cost, last_plat_rate, last_plat_cal_cost, '${month}' month, source
         |from platSide_temp""".stripMargin)

    sql(
      s"""insert overwrite table ${PropUtils.HIVE_TABLE_CATESIDE_COST} partition(month, source)
         |select  load_day, day as data_day, plat, cate_l1, tag_cnt, dup_tag_cnt
         |, cal_cnt, cate_l1_cost, '${month}' month, source
         |from cateSide_temp""".stripMargin)
    this
  }

  def upsert2Mysql(): BaseCarrier = {
    if (!toMysql) return this
    val _jdbc = Jdbcs.of()
    // 更新运营商表
    val carrier = sql(
      s"""select  source, load_day, day as data_day, id_cnt, dup_id_cnt
         |, cal_cnt, carrier_cost
         |from carrierSide_temp""".stripMargin)
    _jdbc.writeToTable(carrier.collect(), "carrier_side_cost", carrier.schema.fieldNames.sorted)

    // 更新业务线表
    val plat = sql(
      s"""select  source, load_day, day as data_day, plat, tag_cnt, dup_tag_cnt
         |, plat_rate, plat_cal_cost, cal_cnt, plat_cost, last_plat_rate, last_plat_cal_cost
         |from platSide_temp""".stripMargin)
    _jdbc.writeToTable(plat.collect(), "plat_side_cost", plat.schema.fieldNames.sorted)

    // 更新行业表
    val cate = sql(
      s"""select  source, load_day, day as data_day, plat, cate_l1, tag_cnt, dup_tag_cnt
         |, cal_cnt, cate_l1_cost
         |from cateSide_temp""".stripMargin)
    _jdbc.writeToTable(cate.collect(), "cate_side_cost", cate.schema.fieldNames.sorted)

    this
  }


}

