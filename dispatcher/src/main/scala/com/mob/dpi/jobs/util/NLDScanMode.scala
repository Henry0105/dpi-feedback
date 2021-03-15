package com.mob.dpi.jobs.util

import java.io.File

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}
import com.mob.dpi.jobs.util.RegexUtil._

class NLDScanMode(root: String) extends ScanMode {


  // henan_mobile/download/data/{load_day}/business_2_result_{data_day}_gen.txt
  override def scan(pattern: PatternRule, fileInfoProducer: FileInfoProducer): List[FileInfo] = {

    val resDir = new File(root, pattern.resultDir).toString
    val mapDir = new File(root, pattern.mappingDir).toString

    val loadDay = pattern.loadDay
    val resultPattern = pattern.resultRegex
    val mappingPattern = pattern.mappingRegex
    println(s"result root dir: ${replaceLoadDay(resDir,loadDay)}")
    // 筛选 loadday 的文件
    val result = FileUtil.listFilesNLD(replaceLoadDay(resDir,loadDay),
      regexDataDay(regexLoadDayTime2(resultPattern,loadDay)),loadDay)
    println(s"mapping root dir: ${replaceLoadDay(mapDir,loadDay)}")
    val mapping = FileUtil.listFilesNLD(replaceLoadDay(mapDir,loadDay), regexDataDay(mappingPattern), loadDay)

    fileInfoProducer.product(pattern, result,mapping)
  }





}
