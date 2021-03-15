package com.mob.dpi.jobs.util

import java.io.File

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}
import com.mob.dpi.jobs.util.RegexUtil._

class DTMode(root: String) extends ScanMode {


  // unicom/download/667673052142845952/{load_day_time1}/generic_20210203_g_{data_day}.txt
  override def scan(pattern: PatternRule, fileInfoProducer: FileInfoProducer): List[FileInfo] = {
    val loadDay = pattern.loadDay
    val mDir = new File(root, pattern.resultDir.substring(0,pattern.resultDir.lastIndexOf("/"))).toString
    val dirPattern  = pattern.resultDir.split("/").last
    val filePattern = pattern.resultRegex
    println("result root dir: " + mDir)
    val files = FileUtil.listFilesDT(mDir,regexLoadDayTime1(dirPattern,loadDay), regexDataDay(filePattern),loadDay)

    fileInfoProducer.product(pattern, files)
  }

}
