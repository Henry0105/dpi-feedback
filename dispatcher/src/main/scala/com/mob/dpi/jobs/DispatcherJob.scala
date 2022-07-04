package com.mob.dpi.jobs

import java.io.File

import com.mob.dpi.jobs.bean.{Params, PatternRule}
import com.mob.dpi.jobs.util.{FileScanner, FileUtil, PropUtil, SenderManager}

case class DispatcherJob(params: Params) {

  val loadDay = params.day
  // 1.文件生成
  val fs = new FileScanner(PropUtil.getProperty("dispatcher.data.root"),
    PropUtil.getProperty("dispatcher.data.hdfs.root"))
  val filePatterns: List[PatternRule] = parsePattern()
  val allTargetFiles = fs.allTargetFiles(filePatterns)


  // 因为hdfs 文件自带校验文件,因此不用自己生成校验文件
  allTargetFiles.filter(!_.scanMode.startsWith("hdfs")).foreach(file => {
    println(s"===> $file")
    // 生成校验文件
    val rPath = file.resultFile
    val mPath = file.mappingFile

    def genFn(p: String): String = {
      p.concat(".dispatcher_verf")
    }

    val resCheckFile = new File(PropUtil.getProperty("dispatcher.check_files"), genFn(rPath)).getAbsolutePath
    val mapCheckFile = new File(PropUtil.getProperty("dispatcher.check_files"), genFn(mPath)).getAbsolutePath


    if (FileUtil.exist(rPath)) {
      FileUtil.checkFileGen(resCheckFile, FileUtil.fileSize(rPath).toString)
      println(s"===> ${resCheckFile}")
    }

    if (FileUtil.exist(mPath)) {
      FileUtil.checkFileGen(mapCheckFile, FileUtil.fileSize(mPath).toString)
      println(s"===> ${mapCheckFile}")
    }

  })


  // 2.启动任务
  val sm = new SenderManager(allTargetFiles)

  sm.init(loadDay)
  sm.start()

  def parsePattern(): List[PatternRule] = {
    PropUtil.toPatternMap().map(
      p => {
        val rule = PatternRule(loadDay, p._1, p._2)
        println(rule)
        rule
      }
    ).toList
  }
}
