package com.mob.dpi.beans

import org.apache.spark.sql.{DataFrame, SparkSession}

class Shandong(comParam: ComParam) extends BaseCarrier(comParam) {


  override def source(spark: SparkSession): DataFrame = {

  spark.sql("")
    spark.emptyDataFrame
  }
}
