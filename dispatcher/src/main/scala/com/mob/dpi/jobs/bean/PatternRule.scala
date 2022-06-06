package com.mob.dpi.jobs.bean

case class PatternRule(loadDay: String, source: String, modelType: String, scanMode: String, producerMode: String,
                       proto: String, resultDir: String, mappingDir: String, resultRegex: String, mappingRegex: String, ori: String) {
}

object PatternRule {
  def apply(loadDay: String, source: String, modelType: String, scanMode: String, producerMode: String, proto: String, resultDir: String, mappingDir: String, resultRegex: String, mappingRegex: String, ori: String): PatternRule = new PatternRule(loadDay, source, modelType, scanMode, producerMode, proto, resultDir, mappingDir, resultRegex, mappingRegex, ori)

  def apply(loadDay: String, key: String, value: String): PatternRule = {

    // dispatcher.subdir.guangdong_new_timewindow.timewindow=norm:guangdong_mobile_new/{load_day}/^.*\\d{8}.txt$
    val items = key.split("\\.")
    val pItems = value.split(":")

    val proto = pItems(0)
    val scanM = proto.split("_")(0)
    val prodM = if (proto.contains("_")) proto.split("_")(1) else "default"

    val paths = pItems(1)

    val Array(resultDir, resultFile) = {
      val temp = paths.split(";")(0)
      Array(temp.substring(0, temp.lastIndexOf("/")), temp.split("/").last)
    }


    val Array(mappingDir, mappingFile) = {
      if (paths.contains(";")) {
        val temp = paths.split(";")(1)
        Array(temp.substring(0, temp.lastIndexOf("/")), temp.split("/").last)
      }
      else {
        Array("","")
      }
    }

    new PatternRule(loadDay, items(2), items(3), scanM, prodM, proto,resultDir,mappingDir,resultFile,mappingFile,s"${key}=${value}")
  }
}
