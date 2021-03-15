package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.TaskInfo
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scalaj.http.{Http, HttpOptions}

class HttpClient {


  def post(task: TaskInfo): String = {

    val json = ("job_name" -> task.taskName) ~ ("task_conf" -> s"${task.params.mkString("[\"","\",\"","\"]")}") ~ ("user_name" -> task.owner)
//    println(compact(render(json)))
    Http("http://scheduler.paas.internal.mob.com/api/startTask")
      .postData(compact(render(json)))
      .header("Content-Type", "application/json")
      .option(HttpOptions.readTimeout(10000))
      .asString.body
  }

  def get(): Unit = {

  }

}
