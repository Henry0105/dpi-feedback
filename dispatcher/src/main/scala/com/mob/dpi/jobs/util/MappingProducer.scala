package com.mob.dpi.jobs.util

import java.io.File

import com.mob.dpi.jobs.bean.{FileInfo, PatternRule}
import com.mob.dpi.jobs.util.RegexUtil._
import org.apache.commons.lang3.StringUtils

class MappingProducer extends FileInfoProducer {
  override def product(pattern: PatternRule, list: List[String]*): List[FileInfo] = {
    val result = list(0)
    val mapping = list(1)

    // 以结果数据为导向
    result.map(p =>
      FileInfo(pattern.loadDay, pattern.source, pattern.modelType, "", p, mappingFile(p, mapping, pattern), pattern.scanMode, pattern.producerMode))
      // 只取 res 和 对应mapping 同时存在的元
      .filter(f=> StringUtils.isNoneBlank(f.mappingFile))
  }

  def mappingFile(result: String, mapping: List[String], pattern: PatternRule): String = {

    val resFileName = new File(result).getName

    val dataDay = matchDataDay(resFileName, pattern)

    val temp = mapping
      .filter(mf => new File(mf).getName.equalsIgnoreCase(pattern.mappingRegex.replace("{data_day}", dataDay)))
    if (temp.isEmpty) {
      ""
    } else {
      temp.head
    }

  }

  def matchDataDay(name: String, pattern: PatternRule): String = {
    val p = regexLoadDayTime2(regexDataDay(pattern.resultRegex), pattern.loadDay)
    p.r.findAllIn(name).group(1)
  }
}
