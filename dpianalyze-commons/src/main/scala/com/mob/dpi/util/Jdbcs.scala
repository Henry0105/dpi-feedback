package com.mob.dpi.util

object Jdbcs {

  def of(): JdbcTools = {
    val ip: String = ApplicationUtils.DPI_COST_JDBC_MYSQL_HOST
    val port: Int = ApplicationUtils.DPI_COST_JDBC_MYSQL_PORT.toInt
    val user: String = ApplicationUtils.DPI_COST_JDBC_MYSQL_USERNAME
    val pwd: String = ApplicationUtils.DPI_COST_JDBC_MYSQL_PASSWORD
    val db: String = ApplicationUtils.DPI_COST_JDBC_MYSQL_DB
    of(ip, port, user, pwd, db)
  }

  def of(jdbcHostname: String, jdbcPort: Int, jdbcUsername: String, jdbcPassword: String,
         jdbcDatabase: String): JdbcTools = {
    JdbcTools(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
  }

}
