package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}

class DefaultProducer extends FileInfoProducer {
  override def product(pattern: PatternRule, list: List[String]*): List[FileInfo] = {
    val single = list(0)
    single
      .map(p => FileInfo(pattern.loadDay, pattern.source, pattern.modelType, "", p, "", pattern.scanMode, pattern.producerMode))
  }
}
