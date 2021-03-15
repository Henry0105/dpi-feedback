package com.mob.dpi.jobs.util

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import org.apache.log4j.Logger

import scala.collection._
import scala.util.{Failure, Success, Try}

object PropUtil {

  private[this] lazy val logger: Logger = Logger.getLogger(PropUtil.getClass)

  private[this] lazy val prop: Properties = {
    val _prop = new Properties()
    var propIn: InputStreamReader = null

    var propFile = "dispatcher.properties"
    Try {
      val clientPropFile = s"${System.getenv("DPI_DISPATCHER_HOME")}/conf/${propFile}"
      propIn = if (new File(clientPropFile).exists()) {
        propFile = clientPropFile
        new InputStreamReader(new FileInputStream(propFile))
      } else {
        new InputStreamReader(PropUtil.getClass.getClassLoader.getResourceAsStream(propFile), "UTF-8")
      }
      _prop.load(propIn)
      propIn.close()
    } match {
      case Success(_) =>
        logger.info(s"props loaded succeed from [$propFile], {${_prop}}")
        _prop
      case Failure(ex) =>
        if (propIn != null) propIn.close()
        throw new InterruptedException(s"配置文件[$propFile]读取错误, ex => " + ex.getMessage)
    }
  }

  def getProperty(key: String): String = {
    prop.getProperty(key)
  }

  def getProperty(): Properties = {
    prop
  }

  def toPatternMap(): Map[String, String] = {
    val it = prop.entrySet().iterator()
    val mapTemp = mutable.Map.empty[String, String]
    while (it.hasNext) {
      val es = it.next()
      if (es.getKey.asInstanceOf[String].startsWith("dispatcher.subdir")) {
        mapTemp.put(es.getKey.asInstanceOf[String], es.getValue.asInstanceOf[String])
      }
    }
    mapTemp.toMap
  }

  def jdbcProperties(): Properties = {
    val it = prop.entrySet().iterator()
    val p = new Properties()

    while (it.hasNext) {
      val es = it.next()
      if (es.getKey.asInstanceOf[String].startsWith("dispatcher.db")) {
        p.setProperty(jdbcKey(es.getKey.asInstanceOf[String]), es.getValue.asInstanceOf[String])
      }
    }
    p
  }


  private def jdbcKey(key: String): String = {
    key.substring("dispatcher.db.".length)
  }

  def main(args: Array[String]): Unit = {


    println(toPatternMap.toString())
  }
}
