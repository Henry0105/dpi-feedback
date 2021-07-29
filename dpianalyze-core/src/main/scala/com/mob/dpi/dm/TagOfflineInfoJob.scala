package com.mob.dpi.dm

import com.mob.dpi.JobContext
import com.mob.dpi.beans.CarrierFactory

/**
 * 1.统计回流表中对应运营商的tag是否下线
 */

case class TagOfflineInfoJob(cxt: JobContext) {


  def run(): Unit = {

    try {
      CarrierFactory.createCarrier(cxt).offlineVerification()
    } finally {
      cxt.spark.stop()
    }

  }


}
