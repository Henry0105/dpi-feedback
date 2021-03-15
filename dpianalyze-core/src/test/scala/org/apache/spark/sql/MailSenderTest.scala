package org.apache.spark.sql

import com.mob.mail.MailSender

object MailSenderTest {

  def main(args: Array[String]): Unit = {
    MailSender.sendMail(s"MailSenderTest",
      s"MailSenderTest",
      "fangym@mob.com")

    println("mail success")
  }
}
