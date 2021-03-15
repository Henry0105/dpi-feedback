package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}

trait ScanMode {

  def scan(pattern: PatternRule, fileInfoProducer: FileInfoProducer): List[FileInfo]


}
