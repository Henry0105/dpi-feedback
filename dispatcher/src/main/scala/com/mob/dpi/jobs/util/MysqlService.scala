package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.{FileInfo, FileInfoBo}

import scala.collection.mutable

class MysqlService {

  val table = PropUtil.getProperty("dispatcher.app.table")

  def save(buf: mutable.Buffer[FileInfoBo]): Unit = {

    SqlProxy.batchInsert(table, buf)

  }

  def update(): Unit = {

  }

  def loadData(loadDay: String): Set[String] = {


    SqlProxy.query(s"select abs_file_path from ${table} where load_day='${loadDay}'")
      .map(_.get("abs_file_path")).filter(_.isDefined).map(_.get.asInstanceOf[String]).toSet

  }
}
