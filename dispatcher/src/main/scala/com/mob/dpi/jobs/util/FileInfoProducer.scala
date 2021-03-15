package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}

trait FileInfoProducer {

  def product(pattern:PatternRule,list: List[String]*): List[FileInfo]
}
