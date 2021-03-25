package com.mob.dpi

import com.mob.dpi.jobs.DispatcherJob
import com.mob.dpi.jobs.bean.Params
import com.mob.dpi.jobs.enums.JobTypeEnum
import com.mob.dpi.jobs.enums.JobTypeEnum._
import scopt.OptionParser

object Bootstrap {


  def main(args: Array[String]): Unit = {


    val parser = new OptionParser[Params]("dpi-common") {

      head("dpi-common")

      opt[Int]("job").abbr("j")
        .text("1: dispatcher")
        .required()
        .action((x, c) => c.copy(job = x))

      opt[String]("day").abbr("d")
        .text("cal date with yyyyMMdd format, default: current date")
        .required()
        .action((x, c) => c.copy(day = x))

    }

    parser.parse(args, Params()) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }

  }

  def run(params: Params): Unit = {
    JobTypeEnum(params.job) match {
      case DISPATCHER => DispatcherJob(params)
      case _ =>
        throw new UnsupportedOperationException(s"unsupported job[${params.job}]")
    }
  }
}
