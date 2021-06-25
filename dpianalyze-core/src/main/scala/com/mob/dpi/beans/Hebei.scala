package com.mob.dpi.beans

import org.apache.spark.sql.SparkSession

case class Hebei(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier() {

  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.006

  override def incrSrcSql: String = {
    s"""
       |select source, load_day, day, model_type, split(get_json_object(data,'$$.data'),'\\\\|')[0] as id
       |from ${incrTab}
       |where source = '${carrier}' and load_day >= '${startDay}' and load_day <= '${endDay}'
       |and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |""".stripMargin
  }

  // 按设备方式计费 (日,id数量)
  override def platSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW platSide_temp as
       |select s.source, s.load_day, s.day, s.plat
       |, tag_cnt
       |, dup_tag_cnt
       |, t.plat_curr_sum/t.carrier_curr_sum plat_rate
       |, round(tag_cnt * (t.plat_curr_sum/t.carrier_curr_sum * ${calPrice} + ${dataPrice}), 4) plat_cost
       |from
       |(
       |  select a.source, a.load_day, a.day, a.plat
       |  , count(1) tag_cnt
       |  , count(distinct id) dup_tag_cnt
       |  from
       |  (
       |      select a.source, a.load_day, a.day, a.id, b.plat
       |      from tagTab_temp a
       |      join mappingTab_temp b
       |      on a.tag = b.tag
       |  )a
       |  group by a.source, a.load_day, a.day, a.plat
       |)s join platDistribution_temp t
       |on s.source = t.source and s.load_day = t.load_day and s.day = t.day and s.plat = t.plat
       |""".stripMargin
  }


}
