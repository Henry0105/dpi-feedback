package com.mob.dpi.beans

import com.mob.dpi.util.PropUtils
import org.apache.spark.sql.SparkSession

case class Hebei(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier {

  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.006

  override def incrSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW incrTab_temp as
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


  // tag offline start

  override val excludeModelOF = Set("timewindow")
}
