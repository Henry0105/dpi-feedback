package com.mob.dpi.beans

import org.apache.spark.sql.SparkSession

case class UnicomProxy(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None) extends BaseCarrier {
  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.004

}
