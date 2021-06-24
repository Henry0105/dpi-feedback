package com.mob.dpi.beans

import org.apache.spark.sql.{DataFrame, SparkSession}

class Unicom(override val comParam: ComParam, option: Option[SparkSession] = None) extends BaseCarrier(option) {

  override def source(spark: SparkSession): DataFrame = {


    spark.sql("")


    spark.emptyDataFrame
  }


  // 按设备方式计费 (日,id数量)
  override def platSideCost: String = {
    s"""
       |select a.source, a.load_day, a.day, a.model_type, a.plat, count(1) tag_cnt, count(distinct id) dup_tag_cnt
       |from
       |(
       |    select a.source, a.load_day, a.day, a.model_type, a.id, b.plat
       |    from tagTab_temp a
       |    join mappingTab_temp b
       |    on a.tag = b.tag
       |)a
       |group by a.source, a.load_day, a.day, a.model_type, a.plat
       |""".stripMargin
  }


}
