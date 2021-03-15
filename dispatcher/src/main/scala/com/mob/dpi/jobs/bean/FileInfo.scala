package com.mob.dpi.jobs.bean

case class FileInfo(override val loadDay: String, override val source: String, override val modelType: String,
                    override val day: String, resultFile: String, mappingFile: String,
                    override val scanMode: String, override val producerMode: String) extends ModelInfo(
  loadDay, source, modelType, day, scanMode, producerMode
)
