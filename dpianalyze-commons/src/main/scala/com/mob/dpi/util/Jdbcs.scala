package com.mob.dpi.util

object Jdbcs {

  def of(): JdbcTools = {
    val businessName = "cost"
//    val ip: String = PropUtils.getProperty(s"$businessName.mysql.ip")
//    val port: Int = PropUtils.getProperty(s"$businessName.mysql.port").toInt
//    val user: String = PropUtils.getProperty(s"$businessName.mysql.user")
//    val pwd: String = PropUtils.getProperty(s"$businessName.mysql.password")
//    val db: String = PropUtils.getProperty(s"$businessName.mysql.database")
    val ip: String = "10.21.33.28"
    val port: Int = 3306
    val user: String = "root"
    val pwd: String = "mobtech2019java"
    val db: String = "dpi_analyse_test"
    of(ip, port, user, pwd, db)
  }

  def of(jdbcHostname: String, jdbcPort: Int, jdbcUsername: String, jdbcPassword: String,
         jdbcDatabase: String): JdbcTools = {
    JdbcTools(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
  }

}
