package com.mob.dpi.jobs.util

object RegexUtil {


  def replaceLoadDay(str: String,loadDay:String): String = {
    str.replace("{load_day}", loadDay)
  }

  def regexDataDay(str: String): String = {
    str.replace("{data_day}", s"(\\d{8})")
  }

  def regexLoadDayTime2(str: String, loadDay: String): String = {
    str.replace("{load_day_time2}", s"(${loadDay}\\d{6})")
  }

  def regexLoadDayTime1(str: String, loadDay: String): String = {
    str.replace("{load_day_time1}", s"(${loadDay}-\\d{6})")
  }
}
