package com.mob.dpi.biz

import com.mob.dpi.util.ApplicationUtils
import org.apache.commons.lang3.StringUtils


trait MailService {

  val mailDefault = ApplicationUtils.DPI_TAG_OFFLINE_MAIL_DEFAULT
  val mailCc = ApplicationUtils.DPI_TAG_OFFLINE_MAIL_CC

  // tag offline

//  val offlineTitle = ""

  def htmlFmt[T](data: Seq[T], heads: String*): String = {

    s"""
       |<table border="1">
       |<tr>${heads.map(h=>s"<th>${h}</th>").mkString("")}</tr>
       |${htmlDataFmt(data,heads:_*)}
       |</table>
     """.stripMargin
  }


  private def htmlDataFmt[T](data: Seq[T], fields: String*):String={
    data.map(obj => {
      fields.map(f =>{
        val _f = obj.getClass.getDeclaredField(f)
        _f.setAccessible(true)
        s"<td>${_f.get(obj)}</td>"} ).mkString("<tr>","","</tr>")
    }).mkString("\n")
  }


  def maybeMailDefault(mail:String):String={
    if(StringUtils.isBlank(mail)) {mailDefault} else {mail}
  }
}
