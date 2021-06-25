package com.mob.dpi.beans

import org.apache.spark.sql.SparkSession

case class Jiangsu(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier() {

  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.005

  override def incrSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW incrTab_temp as
       |select source, load_day, day, id, explode_outer(split(tag, ',')) tag_exp
       |from ${incrTab}
       |where source = '${carrier}' and load_day > '${startDay}' and load_day <= '${endDay}'
       |and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |""".stripMargin
  }
}
