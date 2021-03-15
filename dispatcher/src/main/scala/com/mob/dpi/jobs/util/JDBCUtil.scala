package com.mob.dpi.jobs.util

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSourceFactory

object JDBCUtil {

  private val ds = DruidDataSourceFactory.createDataSource(PropUtil.jdbcProperties())

  def getConnection(): Connection = {
    ds.getConnection
  }


  def main(args: Array[String]): Unit = {

  }
}
