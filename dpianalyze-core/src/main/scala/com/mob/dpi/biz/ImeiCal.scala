package com.mob.dpi.biz

class ImeiCal extends ICostCal {

  override def costCal(): String = {
    s"""
       |select a.source, a.load_day, a.day, a.model_type, a.plat, count(1) tag_cnt, count(distinct id) price_cnt
       |from
       |(
       |    select a.source, a.load_day, a.day, a.model_type, a.tag, a.id, b.plat
       |    from tag_tab_temp a
       |    join mapping_tab b
       |    on a.tag = b.tag
       |)a
       |group by a.source, a.load_day, a.day, a.model_type, a.plat
       |""".stripMargin
  }
}
