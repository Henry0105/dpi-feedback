package com.mob.dpi.dm

import com.mob.dpi.JobContext
import com.mob.dpi.beans.{CarrierFactory, ComParam}
import com.mob.dpi.traits.Cacheable
import org.apache.spark.sql.SparkSession

/**
 * 1.统计incr表
 * 2.统计tag result 表
 *
 */

case class CostStatisticsJob(cxt: JobContext)  {


  def run(): Unit = {

    CarrierFactory.createCarrier(cxt).process().insertIntoHive()
//      .insertIntoMysql()

    cxt.spark.stop()

  }






}
