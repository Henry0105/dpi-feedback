package com.mob.dpi.beans

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseCarrier(comParam: ComParam) {




  def source(spark: SparkSession): DataFrame
}

class CarrierFactory[T] {

  def createCarrier(clazz : Class[T]) : T = {


    clazz.
  }
}