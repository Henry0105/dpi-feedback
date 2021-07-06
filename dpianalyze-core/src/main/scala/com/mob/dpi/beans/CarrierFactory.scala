package com.mob.dpi.beans

import com.mob.dpi.JobContext
import com.mob.dpi.enums.SourceType
import com.mob.dpi.enums.SourceType._
import com.mob.dpi.util.PropUtils

object CarrierFactory {


  def createCarrier(implicit cxt: JobContext): BaseCarrier = {

    SourceType.withName(cxt.params.source) match {
      case SHANDONG => createShandong
      case ANHUI => createAnhui
      case GUANGDONG => createGuangdong
      case HEBEI => createHebei
      case HENAN => createHenan
      case JIANGSU => createJiangsu
      case TELECOM => createTelecom
      case TIANJIN => createTianjin
      case UNICOM => createUnicom
      case ZHEJIANG => createZhejiang
      case SICHUAN => createSichuan
    }

  }

  private def createShandong(implicit cxt: JobContext): Shandong = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_SD}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Shandong(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createAnhui(implicit cxt: JobContext): Anhui = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Anhui(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createGuangdong(implicit cxt: JobContext): Guangdong = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_GD}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Guangdong(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createHebei(implicit cxt: JobContext): Hebei = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "timewindow") ++ cxt.otherArgs
    Hebei(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createHenan(implicit cxt: JobContext): Henan = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "timewindow") ++ cxt.otherArgs
    Henan(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createJiangsu(implicit cxt: JobContext): Jiangsu = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Jiangsu(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createTelecom(implicit cxt: JobContext): Telecom = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_TELECOM}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Telecom(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createTianjin(implicit cxt: JobContext): Tianjin = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Tianjin(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createUnicom(implicit cxt: JobContext): Unicom = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "timewindow") ++ cxt.otherArgs
    Unicom(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createZhejiang(implicit cxt: JobContext): Zhejiang = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Zhejiang(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }

  private def createSichuan(implicit cxt: JobContext): Sichuan = {
    val other = Map("local" -> "false", "incrTab" -> s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON}",
      "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "") ++ cxt.otherArgs
    Sichuan(ComParam(cxt.params.day, cxt.params.source, cxt.params.modelType, cxt.params.day, other), Some(cxt.spark))
  }
}
