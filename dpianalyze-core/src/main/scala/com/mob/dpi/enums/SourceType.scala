package com.mob.dpi.enums

object SourceType extends Enumeration {

  val UNICOM: SourceType.Value = Value(1, "unicom")
  val SHANDONG: SourceType.Value = Value(2, "shandong_mobile")
  val HENAN: SourceType.Value = Value(3, "henan_mobile")
  val SICHUAN: SourceType.Value = Value(4, "sichuan_mobile")
  val ANHUI: SourceType.Value = Value(5, "anhui_mobile")
  val GUANGDONG: SourceType.Value = Value(6, "guangdong_mobile")
  val JIANGSU: SourceType.Value = Value(7, "jiangsu_mobile")
  val ZHEJIANG: SourceType.Value = Value(8, "zhejiang_mobile")
  val TIANJIN: SourceType.Value = Value(9, "tianjin_mobile")
  val TELECOM: SourceType.Value = Value(10, "telecom")
  val HEBEI: SourceType.Value = Value(11, "hebei_mobile")
  val GDN: SourceType.Value = Value(12, "guangdong_mobile_new")
  val TELECOM_TIMEWINDOW: SourceType.Value = Value(13, "telecom_timewindow")
  val UNICOM_PROXY: SourceType.Value = Value(14, "unicom_proxy")
  //  def main(args: Array[String]): Unit = {
  //    println(SourceType(3))
  //    println(SourceType(3).id)
  //    println(SourceType.withName("sichuan_mobile"))
  //    println(SourceType.withName("sichuan_mobile").id)
  //  }
}
