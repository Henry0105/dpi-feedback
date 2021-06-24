package com.mob.dpi.biz

class TagCal extends ICostCal {

  override def costCal(): String = {
    s"""
       |select source, load_day, day, model_type, plat, sum(cnt3) tag_cnt, sum(cnt4) prince_cnt
       |from
       |(
       |    select a.source, a.load_day, a.day, a.model_type, a.tag, b.cate_l1, b.plat, a.cnt3, a.cnt4
       |    from app_total a
       |    join mapping_tab b
       |    on a.tag = b.tag
       |)a
       |group by source, load_day, day, model_type, plat
       |""".stripMargin
  }
}
