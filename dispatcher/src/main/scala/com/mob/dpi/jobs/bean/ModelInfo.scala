package com.mob.dpi.jobs.bean

class ModelInfo(val loadDay: String,val source: String,val modelType: String,val day: String,val scanMode: String,val producerMode: String) {

  def taskKeyGen(): String = {
    source + "." + modelType
  }

}
