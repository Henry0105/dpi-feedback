package com.mob.dpi.beans

import org.apache.spark.sql.{DataFrame, SparkSession}

class Shandong(override val comParam: ComParam, option: Option[SparkSession] = None) extends BaseCarrier(option) {




  override def source(spark: SparkSession): DataFrame = {


  spark.sql("")


    spark.emptyDataFrame
  }







}
