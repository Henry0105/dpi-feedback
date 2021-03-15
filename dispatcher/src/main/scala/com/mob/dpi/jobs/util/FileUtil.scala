package com.mob.dpi.jobs.util

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

object FileUtil {

  val sdf = new SimpleDateFormat("yyyyMMdd")

  def listFiles(path: String, filePattern: String, zeroFilter: Boolean = true): List[String] = {

    val dir = new File(path)

    if (dir.isDirectory) {
      dir.listFiles(f => f.getName.matches(filePattern))
        .filter(!zeroFilter || _.getPath.length > 0)
        .map(f2 => f2.getAbsolutePath)
        .toList
    } else {
      List.empty[String]
    }

  }

  def listFilesNLD(path: String, filePattern: String, loadDay: String, zeroFilter: Boolean = true): List[String] = {

    val dir = new File(path)

    if (dir.isDirectory) {
      dir.listFiles(f => sdf.format(new Date(f.lastModified())).equalsIgnoreCase(loadDay) && f.getName.matches(filePattern))
        .filter(!zeroFilter || _.getPath.length > 0)
        .map(f2 => f2.getAbsolutePath).toList
    } else {
      List.empty[String]
    }

  }

  def listFilesDT(path: String, dirPattern: String, filePattern: String, loadDay: String, zeroFilter: Boolean = true): List[String] = {

    val dir = new File(path)
    if (dir.isDirectory) {
      dir.listFiles(d => sdf.format(new Date(d.lastModified())).equalsIgnoreCase(loadDay) && d.getName.matches(dirPattern))
        .flatMap(dir => {
          dir.listFiles(f => f.getName.matches(filePattern))
            .filter(!zeroFilter || _.getPath.length > 0)
            .map(_.getAbsolutePath)
        }).toList
    } else {
      List.empty[String]
    }
  }

  def checkFileGen(fileName: String, content: String): Unit = {
    var writer: FileWriter = null
    val file = new File(fileName)
    if (!file.getParentFile.exists()) {
      file.getParentFile.mkdirs()
    }

    try {
      writer = new FileWriter(fileName, false)
      writer.write(content)
      writer.flush()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("checkFileGen error.")
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
  }

  def exist(path: String): Boolean = {
    val file = new File(path)
    file.isFile && file.exists()
  }

  def fileSize(path: String): Long = {
    val file = new File(path)
    file.length()
  }
}
