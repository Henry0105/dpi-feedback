package com.mob.dpi.beans

import org.apache.spark.sql.SparkSession

case class Sichuan(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier {
  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.004


  override def incrSrcSql: String = {
    s"""
       |CREATE OR REPLACE TEMPORARY VIEW incrTab_temp as
       |select source, load_day, day, model_type, split(get_json_object(data,'$$.data'),'\\\\|')[3] as id
       |from ${incrTab}
       |where source = '${carrier}' and load_day >= '${startDay}' and load_day <= '${endDay}'
       |and model_type not in ('${outOfModels.split(",").mkString("','")}')
       |""".stripMargin
  }
}
