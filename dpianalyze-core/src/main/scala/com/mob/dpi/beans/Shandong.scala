package com.mob.dpi.beans

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Shandong(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier() {

  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.007

}
