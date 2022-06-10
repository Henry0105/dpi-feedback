package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.{FileInfo, FileInfoBo, FileInfoWithAggr, TaskInfo}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.{mutable, _}
import scala.util.control.Breaks

class SenderManager(list: List[FileInfo]) {

  val client = new HttpClient

  val mysqlService = new MysqlService

  var mataData = Set.empty[String]

  val waiting_send_tasks = mutable.Buffer.empty[TaskInfo]

  val successful = List.empty[FileInfo]

  def init(loadDay: String): Unit = {

    mataData = mysqlService.loadData(loadDay)

  }

  def start(): Unit = {

    tasksGenV2()

    launchTasks()
  }


  // 单文件的任务
  def tasksGen(): Unit = {


    list.filter(fi => {
      !mataData.contains(fi.resultFile)
    }).foreach {
      case defaultFileInfo if defaultFileInfo.producerMode.startsWith("default") =>
        val params = mutable.Buffer.empty[String]
        params += defaultFileInfo.loadDay
        params += defaultFileInfo.modelType
        params += defaultFileInfo.resultFile
        waiting_send_tasks += TaskInfo(PropUtil.getProperty("dispatcher.task." + defaultFileInfo.taskKeyGen()),
          params.toList, "fangym", defaultFileInfo)
      case mappingFileInfo if mappingFileInfo.producerMode.startsWith("mapping") =>
        val params = mutable.Buffer.empty[String]
        params += mappingFileInfo.loadDay
        params += mappingFileInfo.modelType
        params += mappingFileInfo.resultFile + "," + mappingFileInfo.mappingFile
        if (mappingFileInfo.resultFile.nonEmpty && mappingFileInfo.mappingFile.nonEmpty) {
          waiting_send_tasks += TaskInfo(PropUtil.getProperty("dispatcher.task." + mappingFileInfo.taskKeyGen()),
            params.toList, "fangym", mappingFileInfo)
        }
    }

  }

  // 聚合文件的任务
  def tasksGenV2(): Unit = {

    case class FileKey(source: String, modelType: String)

    list.filter(fi => {
      !mataData.contains(fi.resultFile)
    }).groupBy[FileKey](f => {
      FileKey(f.source, f.modelType)
    }).mapValues(vs => {
      val v = vs.head
      val rmFiles = vs.map(v => {
        v.producerMode match {
          case pm1: String if pm1.startsWith("default") => s"${v.resultFile}"
          case pm2: String if pm2.startsWith("mapping") => s"${v.resultFile},${v.mappingFile}"
        }
      }).mkString(" ")
      FileInfoWithAggr(v.loadDay, v.source, v.modelType, v.day, rmFiles, v.scanMode, v.producerMode)
    }).values.foreach {
      case fileInfoWithAggr: FileInfoWithAggr =>
        val params = mutable.Buffer.empty[String]
        params += fileInfoWithAggr.loadDay
        params += fileInfoWithAggr.modelType
        params += fileInfoWithAggr.rmFiles
        waiting_send_tasks += TaskInfo(PropUtil.getProperty("dispatcher.task." + fileInfoWithAggr.taskKeyGen()),
          params.toList, "fangym", fileInfoWithAggr)
      case _ =>
        println("no match FileInfoWithAggr.")
    }

  }

  def launchTasks(): Unit = {
    val loop = new Breaks
    if (waiting_send_tasks.isEmpty) {
      println("no task need to request.")
    } else {
      println(s"waiting_send_tasks size [${waiting_send_tasks.size}]")
      for (req <- waiting_send_tasks) {
        var resp: JValue = null
        var status: String = ""
        var retry = 1
        // 重试 3 次
        loop.breakable {
          while (retry <= 3) {
            println(s"[DO POST]请求-${retry}-次:\nreq: ${req}")
            resp = parse(client.post(req))
            status = compact(render(resp \ "status"))
            if (status.equalsIgnoreCase("0")) {
              loop.break
            }
            Thread.sleep(1000)
            retry += 1
          }
        }
        saveReqInfo(req, resp)
      }
    }
  }

  def saveReqInfo(req: TaskInfo, resp: JValue): Unit = {


    val status = compact(render(resp \ "status"))

    if (status.equalsIgnoreCase("0")) {
      val jobId = compact(render(resp \ "data" \ "job_id"))
      val taskId = compact(render(resp \ "data" \ "task_id"))

      def saveDetail(fileInfo: FileInfo): Unit = {
        val bos = mutable.Buffer.empty[FileInfoBo]
        bos += FileInfoBo(fileInfo.loadDay,
          fileInfo.source,
          fileInfo.modelType,
          fileInfo.day,
          SqlProxy.mysql_zy(fileInfo.resultFile),
          "result",
          taskId,
          req.taskName,
          req.params.mkString(","),
          req.owner,
          status)

        if (!fileInfo.mappingFile.isEmpty) {
          bos += FileInfoBo(fileInfo.loadDay,
            fileInfo.source,
            fileInfo.modelType,
            fileInfo.day,
            SqlProxy.mysql_zy(fileInfo.mappingFile),
            "mapping",
            taskId,
            req.taskName,
            req.params.mkString(","),
            req.owner,
            status)
        }
        println(s"save task [${taskId}] info.")
        mysqlService.save(bos)
      }

      req.detail match {
        case fi: FileInfo =>
          saveDetail(fi)
        case fiaggr: FileInfoWithAggr =>
          val infoes = fiaggr.convertToFileInfos()
          infoes.map(saveDetail(_))
      }

    } else {
      println(s"Request[${req}]\nTask no start, skip save.")
    }
  }


}
