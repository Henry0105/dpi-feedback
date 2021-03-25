package com.mob.dpi.jobs.util

import java.io.File

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}
import com.mob.dpi.jobs.util.RegexUtil._

class HDFSScanMode(root: String) extends ScanMode {


  // root hdfs://ShareSdkHadoop/user/dpi/download
  // load_day=20210318/source=hebei_mobile/model_type=timewindow/day=20210311/txt.20210318120812.txt
  override def scan(pattern: PatternRule, fileInfoProducer: FileInfoProducer): List[FileInfo] = {

    val loadDay = pattern.loadDay
    val resDir = root.concat("/").concat(pattern.resultDir).concat("/").concat(pattern.resultRegex)
    val regexRes = regexDataDay(replaceLoadDay(resDir, loadDay))

    println(s"hdfs success rule: ${regexRes}")

    // 扫描root下所有匹配文件
    val sPath = HDFSUtil.hdfs_lsr(root, 10).filter(kv => {
      kv._2.matches(regexRes)
    })

    println(s"hdfs success path: ${sPath.map(_._2).mkString("\n")}")

    val resFiles = sPath.flatMap(sFile => {
      // 1.判断day的success.20210316135415.txt 是否存在
      if (HDFSUtil.hdfs_exists(sFile._2)) {
        // 2.返回写完的 txt.20210316135415.txt 文件
        val dir = sFile._2.split("/").init.mkString("/")
        HDFSUtil.hdfs_lsr_filter(dir, 1,  _.split("/").last.startsWith("txt"))
          .filter(_._1 > 0)
          .map(_._2)
      } else {
        List.empty[String]
      }
    })

    println(s"hdfs res files : ${resFiles.mkString("\n")}")

    // 3.返回FileInfo
    fileInfoProducer.product(pattern, resFiles)
  }

}
