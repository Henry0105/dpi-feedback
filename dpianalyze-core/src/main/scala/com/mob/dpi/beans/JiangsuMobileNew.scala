package com.mob.dpi.beans

import org.apache.spark.sql.SparkSession

/**
 * @author jiangle
 * @version 1.2.7
 * @Description TODO
 * @createTime 2022年06月13日 11:28:00
 */
case class JiangsuMobileNew(override val comParam: ComParam, override val sparkOpt: Option[SparkSession] = None)
  extends BaseCarrier {
  override protected val calPrice: BigDecimal = 0.0
  override protected val dataPrice: BigDecimal = 0.007

}