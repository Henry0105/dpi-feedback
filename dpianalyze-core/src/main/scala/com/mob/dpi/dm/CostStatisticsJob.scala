package com.mob.dpi.dm

import com.mob.dpi.JobContext
import com.mob.dpi.traits.Cacheable
import org.apache.spark.sql.SparkSession

/**
 * 1.统计incr表
 * 2.统计tag result 表
 *
 */

class CostStatisticsJob(cxt: JobContext) extends Cacheable {
  override protected val spark: SparkSession = cxt.spark

  def run(): Unit = {



  }

}
