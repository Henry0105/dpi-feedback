package job.test

import com.mob.dpi.beans.{Anhui, BaseCarrier, ComParam, Guangdong, Hebei, Henan, Jiangsu, Shandong, Sichuan, Telecom, Tianjin, Unicom, Zhejiang}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class CostTest extends FunSuite with LocalSparkSession {


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("drop database if exists opdw4_224 CASCADE")
    spark.sql("create database opdw4_224")


    createTable(
      """
        |create table if not exists opdw4_224.carrierSide_cost (
        |load_day string,
        |day string,
        |id_cnt int,
        |dup_id_cnt int,
        |cal_cnt int,
        |carrier_cost DOUBLE
        |)
        |partitioned by(month string, source string)
        |stored as orc
        |""".stripMargin)

    createTable(
      """
        |create table if not exists opdw4_224.platSide_cost (
        |load_day string,
        |day string,
        |plat string,
        |tag_cnt int,
        |dup_tag_cnt int,
        |plat_rate DOUBLE,
        |plat_cal_cost DOUBLE,
        |cal_cnt int,
        |plat_cost DOUBLE,
        |last_plat_rate DOUBLE,
        |last_plat_cal_cost DOUBLE
        |)
        |partitioned by(month string, source string)
        |stored as orc
        |""".stripMargin)

    createTable(
      """
        |create table if not exists opdw4_224.cateSide_cost (
        |load_day string,
        |day string,
        |plat string,
        |cate_l1 string,
        |tag_cnt int,
        |dup_tag_cnt int,
        |cal_cnt int,
        |cate_l1_cost DOUBLE
        |)
        |partitioned by(month string, source string)
        |stored as orc
        |""".stripMargin)


    spark.sql(
      """
        |create table opdw4_224.mappingTab_temp as
        |select '信用卡' as cate_l1,'031' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'k9a' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'idh' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'hg0' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'k99' as tag,'金融线' as plat, '20210603.001' as version
        |union all
        |select '信用卡' as cate_l1,'go4' as tag,'金融线' as plat, '20210603_002' as version
        |union all
        |select '信用卡' as cate_l1,'g2h' as tag,'金融线' as plat, '20210603_003' as version
        |union all
        |select '教育' as cate_l1,'KQ11003540' as tag,'智能增长线_智汇' as plat, '20210603_003' as version
        |union all
        |select '教育' as cate_l1,'KQ11001969' as tag,'智能增长线_智汇' as plat, '20210603_004' as version
        |union all
        |select '教育' as cate_l1,'AH11000901' as tag,'智能增长线_智汇' as plat, '20210603.002' as version
        |union all
        |select '教育' as cate_l1,'TC11000034' as tag,'智能增长线_智汇' as plat, '20210603.001' as version
        |union all
        |select '教育' as cate_l1,'00001' as tag,'智能增长线_智汇' as plat, '20210603_005' as version
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.dpi_mkt_url_withtag as
        |select '信用卡' as cate_l1,'031' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'k9a' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'idh' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'hg0' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'k99' as tag,'金融线' as plat, '20210603.001' as version
        |union all
        |select '信用卡' as cate_l1,'go4' as tag,'金融线' as plat, '20210603_002' as version
        |union all
        |select '信用卡' as cate_l1,'g2h' as tag,'金融线' as plat, '20210603_003' as version
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.tmp_url_operatorstag as
        |select '教育' as cate_l1,'KQ11003540' as tag,'智能增长线_智汇' as plat, '20210603_003' as version
        |union all
        |select '教育' as cate_l1,'KQ11001969' as tag,'智能增长线_智汇' as plat, '20210603_004' as version
        |union all
        |select '教育' as cate_l1,'AH11000901' as tag,'智能增长线_智汇' as plat, '20210603.002' as version
        |union all
        |select '教育' as cate_l1,'TC11000034' as tag,'智能增长线_智汇' as plat, '20210603.001' as version
        |union all
        |select '教育' as cate_l1,'00001' as tag,'智能增长线_智汇' as plat, '20210603_005' as version
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.test1 as
        |select 'shandong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, 'bTiZlRzW4GwDa2pnfBe8Cg==' id, '031' tag
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '100567827' id, 'k9a:8#0$###' tag
        |union all
        |select 'jiangsu_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '4700E41D3646099FB6FAA5F23A761913' id, 'KQ11003540:2,KQ11001969:1' tag
        |union all
        |select 'henan_mobile' source, '20210623' load_day, '20210622' day, 'generic' model_type, '01c532c8417fce65c3cfd5b95e8c979f' id, 'idh:1#0$#' tag
        |union all
        |select 'tianjin_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '236992ca26ce4f8dbc1d60e4a7fbd320' id, 'hg0:1' tag
        |union all
        |select 'zhejiang_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '2ae200f4aee699e9b1b34b0b23e8acc8' id, 'k99:12' tag
        |union all
        |select 'anhui_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, 'D916E7DE944E7408D0189C0BB139EDE7' id, 'AH11000901:20' tag
        |union all
        |select 'telecom' source, '20210623' load_day, '20210622' day, 'common' model_type, '32586' id, 'TC11000034' tag
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.test2 as
        |select 'shandong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, 'bTiZlRzW4GwDa2pnfBe8Cg==' id, '031' tag
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '' tag_limit_version, '100567827' id, 'k9a' tag
        |union all
        |select 'jiangsu_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, '4700E41D3646099FB6FAA5F23A761913' id, 'KQ11003540' tag
        |union all
        |select 'henan_mobile' source, '20210623' load_day, '20210622' day, 'generic' model_type, '' tag_limit_version, '01c532c8417fce65c3cfd5b95e8c979f' id, 'idh' tag
        |union all
        |select 'tianjin_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, '236992ca26ce4f8dbc1d60e4a7fbd320' id, 'hg0' tag
        |union all
        |select 'zhejiang_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, '2ae200f4aee699e9b1b34b0b23e8acc8' id, 'k99' tag
        |union all
        |select 'anhui_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, 'D916E7DE944E7408D0189C0BB139EDE7' id, 'AH11000901' tag
        |union all
        |select 'telecom' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, '32586' id, 'TC11000034' tag
        |union all
        |select 'guangdong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, 'f1ce75974bf78799cd70e5ae47529910' id, 'go4' tag
        |union all
        |select 'hebei_mobile' source, '20210623' load_day, '20210622' day, 'generic' model_type, '' tag_limit_version, 'FFFE95584CF8055F4A36AA6315C1DFF4' id, 'g2h' tag
        |union all
        |select 'sichuan_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, '000370b22bf4cd69cdece324f942b8e1' id, '00001' tag
        |""".stripMargin)


    spark.sql(
      """
        |create table opdw4_224.test3 as
        |select 'guangdong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, 'f1ce75974bf78799cd70e5ae47529910|go4#1177' data
        |""".stripMargin)


    val hb_data = """{"data":"FFFE95584CF8055F4A36AA6315C1DFF4|g2h:1#0$#","day":"20210620","file_name":"20210621_hebei_generic_20210620.txt","flag":"txt","model_type":"generic","source":"hebei_mobile"}"""
    val sichuan_data = """{"day":"20210618","source":"sichuan_mobile","data":"01|01|00001|000370b22bf4cd69cdece324f942b8e1|20210617","flag":"txt","file_name":"hlwg_advertput_20210618_001.txt","model_type":"common"}"""
    spark.sql(
      s"""
         |create table opdw4_224.test4 as
         |select 'hebei_mobile' source, '20210623' load_day, '20210622' day, 'generic' model_type, '${hb_data}' data
         |union all
         |select 'sichuan_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '${sichuan_data}' data
         |""".stripMargin)


  }

  override def afterAll(): Unit = {
    spark.sql("drop database if exists opdw4_224 CASCADE")
    stop()
  }


  def prepare(): Unit = {


  }

  test("cost cal") {

    Shandong(ComParam("20210623", "shandong_mobile", "common", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Unicom(ComParam("20210623", "unicom", "generic", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Jiangsu(ComParam("20210623", "jiangsu_mobile", "common", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Henan(ComParam("20210623", "henan_mobile", "generic", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Tianjin(ComParam("20210623", "tianjin_mobile", "common", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Zhejiang(ComParam("20210623", "zhejiang_mobile", "common", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Anhui(ComParam("20210623", "anhui_mobile", "common", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Telecom(ComParam("20210623", "telecom", "common", "20210622", argsGen("opdw4_224.test1")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Guangdong(ComParam("20210623", "guangdong_mobile", "common", "20210622", argsGen("opdw4_224.test3")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Hebei(ComParam("20210623", "hebei_mobile", "generic", "20210622", argsGen("opdw4_224.test4")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    Sichuan(ComParam("20210623", "sichuan_mobile", "common", "20210622", argsGen("opdw4_224.test4")), Some(spark)).process().insertIntoHive().insertIntoMysql()

    show()
  }

  def argsGen(incr: String) = {
    Map("local" -> "true", "incrTab" -> s"${incr}", "tagTab" -> "opdw4_224.test2",
      "mappingTab1" -> "opdw4_224.dpi_mkt_url_withtag", "mappingTab2" -> "opdw4_224.tmp_url_operatorstag",
      "outOfModels" -> "timewindow", "testDB" -> "opdw4_224") ++ Map("startDay" -> "20210601", "mapTabPre" -> "opdw4_224.mappingTab_temp", "monthType" -> "false", "toMysql" -> "false")
  }

  def show(): Unit = {
    println("res =>")
    spark.sql("select * from opdw4_224.carrierSide_cost").show(false)
    spark.sql("select * from opdw4_224.platSide_cost").show(false)
    spark.sql("select * from opdw4_224.cateSide_cost").show(false)
  }
}
