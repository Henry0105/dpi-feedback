package com.mob.dpi.jobs.bean

case class FileInfoWithAggr(override val loadDay: String, override val source: String, override val modelType: String, override val day: String,
                            rmFiles: String, override val scanMode: String, override val producerMode: String) extends ModelInfo(
  loadDay, source, modelType, day, scanMode, producerMode


) {

  def convertToFileInfos(): List[FileInfo] = {

    rmFiles.split(" ").map(rm => {
      val resFile = rm.split(",")(0)
      val mapFile = if (rm.split(",").length > 1) {
        rm.split(",")(1)
      } else {
        ""
      }
      FileInfo(loadDay, source, modelType, day, resFile, mapFile, scanMode, producerMode)
    }).toList
  }

}


