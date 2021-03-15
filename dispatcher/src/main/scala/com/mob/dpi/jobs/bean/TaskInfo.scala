package com.mob.dpi.jobs.bean

case class TaskInfo(taskName: String, params: List[String], owner: String, detail: ModelInfo) {
  override def toString: String = {
    s"TaskInfo{taskName=${taskName},params=${params.mkString(",")},owner=${owner}"
  }


}
