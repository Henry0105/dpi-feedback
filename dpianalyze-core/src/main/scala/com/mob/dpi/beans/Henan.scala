package com.mob.dpi.beans

import com.mob.dpi.util.PropUtils
import org.apache.spark.sql.SparkSession

case class Henan(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier {

  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.007

  // 按设备方式计费 (日,id数量)
  override def platSideCost: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW platSide_temp as
       |select s.source, s.load_day, s.day, s.plat
       |, tag_cnt
       |, dup_tag_cnt
       |, 0 plat_rate
       |, 0 plat_cal_cost
       |, dup_tag_cnt cal_cnt
       |, round(dup_tag_cnt * ${dataPrice}, 4) plat_cost
       |, 0 last_plat_rate
       |, 0 last_plat_cal_cost
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
       |)s
       |""".stripMargin
  }


}
