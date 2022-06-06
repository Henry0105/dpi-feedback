package com.mob.dpi.beans

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mob.dpi.TagOfflineInfo
import com.mob.dpi.biz.MailService
import com.mob.dpi.traits.Cacheable
import com.mob.dpi.util.{ApplicationUtils, JdbcTools, Jdbcs, PropUtils}
import com.mob.mail.MailSender
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


trait BaseCarrier extends Cacheable with MailService {


  protected val comParam: ComParam
  protected val sparkOpt: Option[SparkSession]

  protected val otherMap: Map[String, String] = comParam.otherArgs.filter(kv => StringUtils.isNoneBlank(kv._2))

  protected val local: Boolean = otherMap.getOrElse("local", "false").toBoolean
  protected val incrTab: String = otherMap.getOrElse("incrTab", "")
  protected val tagTab: String = otherMap.getOrElse("tagTab", "")
  protected val mappingTab1: String = otherMap.getOrElse("mappingTab1", s"${PropUtils.HIVE_TABLE_DPI_MKT_URL_WITHTAG}")
  protected val mappingTab2: String = otherMap.getOrElse("mappingTab2", s"${PropUtils.HIVE_TABLE_TMP_URL_OPERATORSTAG}")
  protected val carrier: String = comParam.source
  protected val endDay: String = comParam.loadDay
  protected val outOfModels = otherMap.getOrElse("outOfModels", "")
  protected val startDay: String = otherMap.getOrElse("startDay", {
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
       |, 0 plat_rate
       |, 0 plat_cal_cost
       |, tag_cnt cal_cnt
       |, round(tag_cnt * ${dataPrice}, 4) plat_cost
       |, 0 last_plat_rate
       |, 0 last_plat_cal_cost
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
       |)s
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
    //    sql(platDistribution)

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

  /*
  CREATE TABLE `carrier_side_cost` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `source` varchar(100) DEFAULT NULL COMMENT '运营商',
  `load_day` varchar(8) DEFAULT NULL COMMENT '计算日期',
  `data_day` varchar(8) DEFAULT NULL COMMENT '数据日期',
  `id_cnt` bigint(20) NOT NULL COMMENT '设备数量',
  `dup_id_cnt` bigint(20) NOT NULL COMMENT '设备数量(去重)',
  `cal_cnt` bigint(20) NOT NULL COMMENT '用于计算的设备出数量',
  `carrier_cost` decimal(24,3) DEFAULT NULL COMMENT '运营商成本(日)',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `sld` (`source`,`load_day`,`data_day`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=37413 DEFAULT CHARSET=utf8;


   */
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

  /**
   * 运营商tag下线校验
   */

  protected val excludeModelOF = Set.empty[String]

  def offlineVerification(): Unit = {

    val _jdbc = Jdbcs.of()
    // 保存每天回流的tag的出数量
    val tagCntDF =
      sql(
        s"""
           |select load_day, source as carrier_en, day as data_day, tag, count(1) id_cnt, count(distinct id) id_dst_cnt
           |from ${tagTab}
           |where load_day='${endDay}' and source='${carrier}'
           |${if (excludeModelOF.isEmpty){"and 1 = 1"} else {s"and model_type not in (${excludeModelOF.mkString("'","','","'")})"} }
           |group by load_day, source, day, tag
           |""".stripMargin)

    val _tagCntDF = tagCntDF.collect()

    if (_tagCntDF.isEmpty) {logger.info(s"${carrier} no data feedback.");return; }

    _jdbc.writeToTable(_tagCntDF, "dpi_tag_cnt_info", tagCntDF.schema.fieldNames.sorted)


    val tagOfflineInfoes = _jdbc.find(
      s"""
         |SELECT
         |  a.id,
         |	a.carrier_zh,
         |	a.carrier_en,
         |	a.tag,
         |	a.effective_day,
         |	a.user_email,
         |	a.check_times
         |FROM
         |	(
         |		SELECT
         |      t1.id,
         |			t1.carrier_id,
         |			t1.carrier_zh,
         |			t1.carrier_en,
         |			t1.shard,
         |			t1.tag,
         |			t1.effective_day,
         |			t1.user_email,
         |			t1.check_times
         |		FROM
         |			dpi_tag_effective_info t1
         |		JOIN (
         |			SELECT
         |				carrier_id,
         |				carrier_zh,
         |				carrier_en,
         |				shard,
         |				tag,
         |				max(update_time) update_time
         |			FROM
         |				dpi_tag_effective_info
         |			WHERE
         |				DATE_FORMAT(update_time, '%Y%m%d') < '${endDay}'
         |			AND carrier_en = '${carrier}'
         |			GROUP BY
         |				carrier_id,
         |				carrier_zh,
         |				carrier_en,
         |				shard,
         |				tag
         |		) t2 ON t1.carrier_id = t2.carrier_id
         |		AND t1.carrier_en = t2.carrier_en
         |		AND t1.carrier_zh = t2.carrier_zh
         |		AND t1.shard = t2.shard
         |		AND t1.tag = t2.tag
         |		AND t1.update_time = t2.update_time
         |		WHERE
         |			STATUS IN ('0','2')
         |		AND check_times < '10000'
         |	) a
         |JOIN (
         |	SELECT
         |		load_day,
         |		carrier_en,
         |		data_day,
         |		tag
         |	FROM
         |		dpi_tag_cnt_info
         |	WHERE
         |		load_day = '${endDay}'
         |	AND carrier_en = '${carrier}'
         |) b ON a.tag = b.tag
         |AND DATE_FORMAT(a.effective_day, '%Y%m%d') <= b.data_day;
         |""".stripMargin, r => {
        TagOfflineInfo(r.getInt("id"), r.getString("carrier_en"), r.getString("carrier_zh"), r.getString("tag"), r.getTimestamp("effective_day"), r.getString("user_email"), r.getInt("check_times"))
      })


    // 邮件发送
    if (tagOfflineInfoes.isEmpty) {
      logger.info("标签都已下线")
    } else {
      logger.info("发送邮件")

      tagOfflineInfoes.groupBy(_.userEmail).foreach(kv => {

        MailSender.sendMail(s"DPI TAG OFFLINE Monitor[load_day=${endDay}]",
          s"${htmlFmt(kv._2,"carrierZh", "carrierEn", "tag", "effectiveDay", "userEmail", "checkTimes")}",
          maybeMailDefault(kv._1),mailCc,"")

        logger.info(s"""${maybeMailDefault(kv._1)},
                ${htmlFmt(kv._2,"carrierZh", "carrierEn", "tag", "effectiveDay", "userEmail", "checkTimes")}""")
      })
    }

    // 更新 mysql 的check_times+1
    val sqls = tagOfflineInfoes.map(info =>
      s"""
         |update dpi_tag_effective_info set check_times = ${info.checkTimes + 1} where id = ${info.id};
         |""".stripMargin).toArray

    if (_jdbc.executeUpdate(sqls)) {
      logger.info("update success.")
    }

  }

}

