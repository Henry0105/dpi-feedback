package com.mob.dpi.helper

import java.sql.Timestamp
import java.util.Date

import com.mob.dpi.helper.MySqlDpiStatHelper.DpiFeedBackStat
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

class StatHelper(@transient val spark: SparkSession) extends Serializable {
  @transient private[this] val logger: Logger = Logger.getLogger(this.getClass)

  case class Param(srcName: String, partition: Map[String, String],
                   dstName: String, feedbackType: String, force: Boolean)

  def getDays(param: Param): Set[String] = {
    getPartitions(param.srcName, param.partition)
      .map(p => {
        val arr = p.split("/")
        if (arr.length >= 4) {
          Some(arr(3))
        } else {
          None
        }
      }).filter(_.nonEmpty).map(pd => parseDay(pd.get)).toSet
  }

  def newSourceStatAndGet(param: Param): DpiFeedBackStat = {
    var srcStats = MySqlDpiStatHelper.getSrcStats(dailySrcQuery(param))
    if (srcStats.isEmpty) {
      val srcStat = build(getDB(param.srcName), getTable(param.srcName), loadDay(param), source(param),
        modelType(param), "", "", "", "", 0)
      MySqlDpiStatHelper.batchInsert(srcStat)
      srcStats = MySqlDpiStatHelper.getSrcStats(dailySrcQuery(param))
    }
    srcStats(0)
  }

  def maybeUpdateSrcStat(originStat: DpiFeedBackStat): Unit = {
    if (originStat.srcDiscoverTime == null) {
      originStat.srcDiscoverTime = new Timestamp(new Date().getTime)
      MySqlDpiStatHelper.updateSrc(originStat)
    }
  }
  def newDpiStatAndGet(param: Param, rowType: Int): Array[DpiFeedBackStat] = {

    val array = getPartitions(param.srcName, param.partition).map(p => {
      val arr = p.split("/")
      val stat = buildPartitionStat(param, arr, rowType)
      stat.srcDiscoverTime = new Timestamp(new Date().getTime)
      stat
    }).toArray

    MySqlDpiStatHelper.batchInsert(array)
    MySqlDpiStatHelper.getPartitionStats(dailyStatsQuery(param, rowType)).toArray
  }

  def buildPartitionStat(param: Param, arr: Array[String], rowType: Int ): DpiFeedBackStat = {
    buildNewObject(getDB(param.srcName), getTable(param.srcName), arr, param.feedbackType,
      getDB(param.dstName), getTable(param.dstName), rowType)
  }

  def dailyStatsQuery(param: Param, rowType: Int): DpiFeedBackStat = {
    DpiFeedBackStat(srcDB = getDB(param.srcName),
      srcTable = getTable(param.srcName), loadDay = loadDay(param),
      source = source(param), modelType = modelType(param),
      dstDB = getDB(param.dstName), dstTable = getTable(param.dstName), rowType = rowType)
  }

  def dailySrcQuery(param: Param): DpiFeedBackStat = {
    DpiFeedBackStat(srcDB = getDB(param.srcName),
      srcTable = getTable(param.srcName), loadDay = loadDay(param),
      source = source(param), modelType = modelType(param), rowType = 0)
  }

  def statAfterCal(stat: DpiFeedBackStat): Unit = {
    stat.calculated = 1
    stat.calCount += 1
    if (stat.calCount == 1) {
      stat.calFirstTime = new Timestamp(new Date().getTime)
    }
    stat.calLastTime = new Timestamp(new Date().getTime)
  }

  def statAfterFail(stat: DpiFeedBackStat): Unit = {
    stat.succeeded = 0
  }

  def statAfterMail(stat: DpiFeedBackStat): Unit = {
    stat.warnMailLastTime = new Timestamp(new Date().getTime)
    stat.succeeded = 0
  }

  def statAfterSuccess(stats: Array[DpiFeedBackStat]): Unit = {
    stats.filter(_.succeeded == 0).foreach(st => {
      if (dstPartitionExist(st)) {
        st.succeeded = 1
      }
    })
  }

  def statBeforeCal(stat: DpiFeedBackStat): Unit = {
    stat.reEntry += 1
  }
  def statNoCal(stat: DpiFeedBackStat): Unit = {

  }
  def needToCal(stat: DpiFeedBackStat): Boolean = {
    stat.calculated == 0
  }

  def finishDpiStatAndSave(stats : Array[DpiFeedBackStat]): Int = {
    MySqlDpiStatHelper.update(stats)
  }

  def loadDay(param: Param): String = {
    param.partition.getOrElse("load_day", "")
  }
  def source(param: Param): String = {
    param.partition.getOrElse("source", "")
  }
  def modelType(param: Param): String = {
    param.partition.getOrElse("model_type", "")
  }

  def parseDay(partitionDay: String): String = {
    partitionDay.split("=")(1)
  }
  def buildNewObject(srcDB: String, srcTable: String, arr: Array[String], feedbackType: String,
                     dstDB: String, dstTable: String, rowType: Int): DpiFeedBackStat = {
    build(srcDB, srcTable, getVal(arr(0)), getVal(arr(1)), getVal(arr(2)),
      getVal(arr(3)), feedbackType, dstDB, dstTable, rowType)
  }

  def build(srcDB: String, srcTable: String, loadDay: String, source: String, modelType: String,
            day: String, feedbackType: String, dstDB: String, dstTable: String, rowType: Int): DpiFeedBackStat = {
    DpiFeedBackStat(
      srcDB = srcDB,
      srcTable = srcTable,
      loadDay = loadDay,
      source = source,
      modelType = modelType,
      day = day,
      feedbackType = feedbackType,
      dstDB = dstDB,
      dstTable = dstTable,
      rowType = rowType,
      statVersion = new Timestamp(new Date().getTime)
    )
  }

  def getVal(str: String): String = {
    getKV(str)._2
  }

  def getKV(str: String): (String, String) = {
    str.split("=")(0)->str.split("=")(1)
  }

  def srcPartitionExist(param: Param): Boolean = {
    getSrcPartitions(param).nonEmpty
  }

  def getSrcPartitions(param: Param): Seq[String] = {
    getPartitions(param.srcName, param.partition)
  }

  def dstPartitionExist(stat: DpiFeedBackStat): Boolean = {
    getDstPartitions(stat).nonEmpty
  }

  def getDstPartitions(stat: DpiFeedBackStat): Seq[String] = {
    getPartitions(stat.dstDB, stat.dstTable, extractPartition(stat))
  }

  def getPartitions(name: String, partitions: Map[String, String]): Seq[String] = {
    getPartitions(getDB(name), getTable(name), partitions)
  }

  def getPartitions(db: String, table: String, partitions: Map[String, String]): Seq[String] = {
    spark.sessionState.catalog.listPartitionNames(TableIdentifier(table, Some(db)), Some(partitions))
  }

  def extractPartition(stat: DpiFeedBackStat): Map[String, String] = {
    Map("load_day" -> stat.loadDay, "source" -> stat.source, "model_type" -> stat.modelType, "day" -> stat.day)
  }

  def getDB(name: String): String = {
    getTableAndDB(name)._1
  }

  def getTable(name: String): String = {
    getTableAndDB(name)._2
  }

  def getTableAndDB(name: String): (String, String) = {
    name.split("\\.")(0)->name.split("\\.")(1)
  }

  def doWithStatus(param: Param, f: DpiFeedBackStat => Unit): Unit = {
    // 创建源的状态
    val originStat = newSourceStatAndGet(param)
    // 创建并返回源子分区状态
    val stats = newDpiStatAndGet(param, 1)
    if (stats.nonEmpty) {
      maybeUpdateSrcStat(originStat)
      stats.foreach(statBeforeCal)
      stats.foreach(stat => {
        if (param.force || needToCal(stat)) {
          f(stat)
          statAfterCal(stat)
        } else {
          statNoCal(stat)
          logger.warn(s"Data has calculated, Skip!")
        }
      })
      // 如果数据正常输出,更新分区状态
      statAfterSuccess(stats)
    } else {
      logger.warn(s"Partition[load_day=${loadDay(param)}/source=${source(param)}/model_type=${modelType(param)}] " +
        s"not exist in table[${param.srcName}]")
    }
    finishDpiStatAndSave(stats)
  }
}
