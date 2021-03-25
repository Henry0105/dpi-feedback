package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}

class FileScanner(root: String, hdfsRoot: String) {

  val scanModeMap = Map("norm" -> new NormScanMode(root),
    "nld" -> new NLDScanMode(root),
    "dt" -> new DTMode(root),
    "hdfs" -> new HDFSScanMode(hdfsRoot),
  )

  val producerMap = Map("mapping" -> new MappingProducer,
    "default" -> new DefaultProducer)

  def allTargetFiles(filePatterns: List[PatternRule]): List[FileInfo] = {

    filePatterns.flatMap{
      case p: PatternRule =>
        scanModeMap(p.scanMode).scan(p, producerMap(p.producerMode))
      case _ => throw new Exception
    }

  }

}
