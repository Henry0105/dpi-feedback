package com.mob.dpi

import com.mob.dpi.dm.{DeviceTagResult, PlatDuid}
import com.mob.dpi.monitor.{DPIDataMonitor, DPIMonitorFromMysqlMetric}
import scopt.OptionParser

/**
 * 程序入口
 */
object Bootstrap {

  def main(args: Array[String]): Unit = {

    val inputParams = Params()

    val parser = new OptionParser[Params](Bootstrap.getClass.getName) {

      head("DPIAnalyze")

      opt[String]("jobs").abbr("j")
        .text("任务执行列表, [1-MappingValue|2-PlatDuid|3-Monitor], 默认: 1")
        .required()
        .action((x, c) => c.copy(jobs = x))

      opt[Unit]("local").abbr("l")
        .text("本地模式, 调试用, 默认: false")
        .action((_, c) => c.copy(local = true))

      opt[String]("day").abbr("d")
        .text("计算日期, yyyyMMdd格式, 默认T-2")
        .action((x, c) => c.copy(day = x))

      opt[String]("model").abbr("m")
        .text("id类型, 支持: idfa|game|common, 默认: game")
        .action((x, c) => c.copy(modelType = x))

      opt[String]("province").abbr("p")
        .text("省份id, 默认: all")
        .action((x, c) => c.copy(province = x))

      opt[String]("source").abbr("s")
        .text("运营商, 支持: unicom, 默认: unicom")
        .action((x, c) => c.copy(source = x))

      opt[Boolean]("mapping").abbr("t")
        .text("是否通过表dm_dpi_mapping_test.dm_dpi_mkt_url_tag_comment_extenal进行tags转换")
        .action((x, c) => c.copy(mapping = x))

      opt[Boolean]("force").abbr("f")
        .text("强制计算, 目标分区会被覆盖")
        .action((x, c) => c.copy(force = x))

      opt[String]("imDay").text("使用中ID_MAPPING的日分区")
        .action((x, c) => c.copy(imDay = x))
    }

    parser.parse(args, inputParams) match {
      case Some(params) =>
        run(params)
      case _ =>
        sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val jobContext = JobContext(params)

    params.jobs match {
      case "1" =>
        DeviceTagResult(jobContext).run()
      case "2" =>
        PlatDuid(jobContext).run()
      case "3" =>
        DPIDataMonitor(jobContext).run()
      case "4" =>
        DPIMonitorFromMysqlMetric(jobContext).run()
      case _ =>
        throw new UnsupportedOperationException(s"unsupported jobs[${ params.jobs }], " +
          s"[1-MappingValue]")
    }
  }
}
