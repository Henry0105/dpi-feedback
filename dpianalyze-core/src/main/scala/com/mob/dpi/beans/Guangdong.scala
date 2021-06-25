package com.mob.dpi.beans

import org.apache.spark.sql.SparkSession

case class Guangdong(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier {

  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.015

  override def incrSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW incrTab_temp as
       |select source, load_day, day, split(data, '\\|')[0] id
       |from ${incrTab}
       |where source = '${carrier}' and load_day >= '${startDay}' and load_day <= '${endDay}'
       |and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |""".stripMargin
  }
}
