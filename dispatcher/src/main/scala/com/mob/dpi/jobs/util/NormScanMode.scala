package com.mob.dpi.jobs.util

import java.io.File

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}
import com.mob.dpi.jobs.util.RegexUtil._

class NormScanMode(root: String) extends ScanMode {


  // henan_mobile/download/data/{load_day}/business_2_result_{data_day}_gen.txt
  override def scan(pattern: PatternRule, fileInfoProducer: FileInfoProducer): List[FileInfo] = {
    val loadDay = pattern.loadDay
    val resDir = new File(root, pattern.resultDir).toString
    val filePattern = pattern.resultRegex
    println(s"result root dir: ${replaceLoadDay(resDir, loadDay)}")
    val files = FileUtil.listFiles(replaceLoadDay(resDir, loadDay), regexDataDay(filePattern))

    fileInfoProducer.product(pattern, files)
  }

}
