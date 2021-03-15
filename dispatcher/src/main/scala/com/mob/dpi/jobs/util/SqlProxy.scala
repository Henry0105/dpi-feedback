package com.mob.dpi.jobs.util

import com.mob.dpi.jobs.bean.FileInfoBo
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object SqlProxy {

  private val log = Logger.getLogger(SqlProxy.getClass)

  def update(sql: String): Int = {
    val connection = JDBCUtil.getConnection()
    try {
      connection.prepareStatement(sql).executeUpdate()
    } catch {
      case e: Throwable => throw e
    } finally {

    }
  }

  def insert(sql: String): Boolean = {
    val connection = JDBCUtil.getConnection()
    try {
      val statement = connection.createStatement()
      statement.execute(sql)
    } catch {
      case e: Throwable => throw e
    } finally {

    }
  }

  def createDispatcherInfo(): Boolean = {
    val connection = JDBCUtil.getConnection()
    try {
      connection.prepareStatement(
        s"""
           |CREATE TABLE IF NOT EXISTS mob_dpi_m_data.dispatcher_info(
           |load_day varchar(8),
           |source varchar(30),
           |model_type varchar(30),
           |day varchar(8),
           |abs_file_path varchar(200),
           |file_type varchar(10),
           |task_id varchar(30),
           |task_name varchar(80),
           |task_conf TEXT,
           |user_name varchar(30),
           |is_start varchar(10),
           |create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
           |update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
           |primary key (load_day,source,model_type,day,abs_file_path,file_type)
           |)
           |""".stripMargin).execute()
    } catch {
      case e: Throwable => throw e
    } finally {

    }

  }

  def insert(table: String, row: FileInfoBo): Unit = {
    val prefix = s"REPLACE INTO ${table}(load_day,source,model_type,day,abs_file_path,file_type,task_id,task_name,task_conf,user_name,is_start) VALUES "
    val suffix = new StringBuffer()
    suffix.append(s"('${row.loadDay}','${row.source}','${row.modelType}','${row.day}','${row.absFilePath}','${row.fileType}','${row.taskId}','${row.taskName}','${row.taskConf}','${row.userName}','${row.isStart}'),")
    val sql = prefix + suffix.substring(0, suffix.length() - 1)
    insert(sql)
    suffix.setLength(0)
  }

  def batchInsert(table: String, rows: mutable.Buffer[FileInfoBo], batch_size: Int = 2000, sleep: Int = 0) {

    val prefix = s"REPLACE INTO ${table}(load_day,source,model_type,day,abs_file_path,file_type,task_id,task_name,task_conf,user_name,is_start) VALUES "

    var suffix = new StringBuffer()

    val connection = JDBCUtil.getConnection()


    connection.setAutoCommit(false)

    val statement = connection.createStatement()

    try {

      var i = 0
      suffix = new StringBuffer()

      rows.foreach(row => {

        suffix.append(s"('${row.loadDay}','${row.source}','${row.modelType}','${row.day}','${row.absFilePath}','${row.fileType}','${row.taskId}','${row.taskName}','${row.taskConf}','${row.userName}','${row.isStart}'),")
        i += 1

        if (i % batch_size == 0) {
          val sql = prefix + suffix.substring(0, suffix.length() - 1)
          statement.addBatch(sql)
          statement.executeBatch()
          connection.commit()
          suffix = new StringBuffer()
          i = 0
          log.info(s"sql insert ${batch_size}")
          if (sleep > 0) Thread.sleep(sleep)

        }
      })

      if (i != 0) {
        val sql = prefix + suffix.substring(0, suffix.length() - 1)
        statement.addBatch(sql)
        statement.executeBatch()
        connection.commit()
        suffix = new StringBuffer()
        i = 0
      }

    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    } finally {
      if (statement != null) {
        try {
          statement.close()
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            throw e
        }
      }
    }

  }

  def query(sql: String): mutable.Buffer[mutable.Map[String, Any]] = {
    val datas = ArrayBuffer.empty[mutable.Map[String, Any]]

    val connection = JDBCUtil.getConnection()

    try {

      val res = connection.prepareStatement(sql).executeQuery()


      val columnCount = res.getMetaData.getColumnCount
      while (res.next()) {

        val row = mutable.Map.empty[String, Any]
        (1 to columnCount).map(c => {
          row += (res.getMetaData.getColumnName(c) -> res.getObject(c))
        })


        datas += row
      }

      datas
    } catch {
      case e: Throwable => throw e
    } finally {

    }

  }

  def mysql_zy(str:String):String={
    str.replace("\\","\\\\")
  }

  def main(args: Array[String]): Unit = {

    val loadDay = "20210207"
    //    createDispatcherInfo()

    //    insert("mob_dpi_m_data.dispatcher_info", FileInfoBo("20210207", "unicom", "generic", "", "unicom/download/667673052142845952/{load_day_time1}/generic_\\\\d{8}_g_{data_day}.txt", "result", "123", "test", "a,b", "ffyymm", "0"))
    //    batchInsert("mob_dpi_m_data.dispatcher_info",mutable.Buffer(FileInfoBo("20210207","unicom","generic","","unicom/download/667673052142845952/{load_day_time1}/generic_\\\\d{8}_g_{data_day}.txt","result","123","test","a,b","ffyymm","0")))
        println(query(s"select * from mob_dpi_m_data.dispatcher_info where load_day='${loadDay}'"))
//    println(query(s"select update_time from mob_dpi_m_data.dispatcher_info where load_day='${loadDay}'"))


  }

}
