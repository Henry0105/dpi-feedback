package com.mob.dpi.jobs

import com.mob.dpi.jobs.bean.TaskInfo
import org.json4s.jackson.Serialization
import org.json4s.native.JsonMethods.{compact, parse, render}
import scalaj.http.{Http, HttpOptions}


object Test {

  def main(args: Array[String]): Unit = {

//    val reqJson = Http("http://scheduler.paas.internal.mob.com/api/startTask")
//      .postData("{\"job_name\":\"dep_test\",\"task_conf\":[\"20210129\"],\"user_name\":\"fangym\"}")
//      .header("Content-Type", "application/json")
//      .option(HttpOptions.readTimeout(10000)).asString.body

//    import org.json4s._
////    import org.json4s.jackson.JsonMethods._
//    import org.json4s.native.JsonMethods._
//    import org.json4s.JsonDSL._
//
//    parse("""{ "numbers" : [1, 2, 3, 4] }""")
//    parse("""{"name":"Toy","price":35.35}""", useBigDecimalForDouble = true)
//
//    val json = List(1, 2, 3)
//
//
//    compact(render(json))
//    println(compact(render(json)))
//
//
//    val json2 = ("name" -> "joe")
//
//    println(compact(render(json2)))
//
//    val json3 = ("name" -> "joe") ~ ("age" -> 35)
//    println(compact(render(json3)))
//
//    val info = TaskInfo("test1", List("1", "2"), "fangym")
//
//    val json4 = ("job_name"-> info.taskName)~("task_conf"->info.params)~("user_name"->info.owner)
//
//    println(compact(render(json4)))
//      new DispatcherJob

//    println(reqJson)
//    val json = parse(reqJson)
//    val jobId= compact(render(json \ "data" \ "job_id"))
//    val taskId = compact(render(json \ "data" \ "task_id"))
//    val status = compact(render(json \ "status"))
//
//    println(jobId)
//    println(taskId)
//    println(status)
  }

}
