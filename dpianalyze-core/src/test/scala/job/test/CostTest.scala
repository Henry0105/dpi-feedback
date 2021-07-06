package job.test

import com.mob.dpi.beans.{Anhui, BaseCarrier, ComParam, Guangdong, Hebei, Henan, Jiangsu, Shandong, Sichuan, Telecom, Tianjin, Unicom, Zhejiang}
import com.mob.dpi.util.{FileUtils, PropUtils}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class CostTest extends FunSuite with LocalSparkSession {

  val scriptDDLDir = """dpianalyze-core/src/test/sql_script/ddl"""

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("drop database if exists dm_dpi_master CASCADE")
    spark.sql("create database dm_dpi_master")

    val carrierTblSql = FileUtils.getSqlScript(s"${scriptDDLDir}/dpi_cost_script.sql",
      tableName = PropUtils.HIVE_TABLE_CARRIERSIDE_COST)
    createTable(carrierTblSql)

    val platTblSql = FileUtils.getSqlScript(s"${scriptDDLDir}/dpi_cost_script.sql",
      tableName = PropUtils.HIVE_TABLE_PLATSIDE_COST)
    createTable(platTblSql)

    val cateTblSql = FileUtils.getSqlScript(s"${scriptDDLDir}/dpi_cost_script.sql",
      tableName = PropUtils.HIVE_TABLE_CATESIDE_COST)
    createTable(cateTblSql)

    val distributionTblSql = FileUtils.getSqlScript(s"${scriptDDLDir}/dpi_cost_script.sql",
      tableName = PropUtils.HIVE_TABLE_PLAT_DISTRIBUTION)
    createTable(distributionTblSql)

    spark.sql(
      """
        |create table dm_dpi_master.mappingTab_temp as
        |select '信用卡' as cate_l1,'031' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'k9a' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '教育' as cate_l1,'k9b' as tag,'智能增长' as plat, '20210603_001' as version
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
        |create table dm_dpi_master.dpi_mkt_url_withtag as
        |select '信用卡' as cate_l1,'031' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '信用卡' as cate_l1,'k9a' as tag,'金融线' as plat, '20210603_001' as version
        |union all
        |select '教育' as cate_l1,'k9b' as tag,'智能增长' as plat, '20210603_001' as version
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
        |create table dm_dpi_master.tmp_url_operatorstag as
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
      s"""
        |create table ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_SD} as
        |select 'shandong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, 'bTiZlRzW4GwDa2pnfBe8Cg==' id, '031' tag
        |""".stripMargin)

    spark.sql(
      s"""
         |create table ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_TELECOM} as
         |select 'telecom' source, '20210623' load_day, '20210622' day, 'common' model_type, '32586' id, 'TC11000034' tag
         |""".stripMargin)

    spark.sql(
      s"""
        |create table ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR} as
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '100567827' id, 'k9a:8#0$$###' tag
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '100567828' id, 'k9b:8#0$$###' tag
        |union all
        |select 'jiangsu_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '4700E41D3646099FB6FAA5F23A761913' id, 'KQ11003540:2,KQ11001969:1' tag
        |union all
        |select 'henan_mobile' source, '20210623' load_day, '20210622' day, 'generic' model_type, '01c532c8417fce65c3cfd5b95e8c979f' id, 'idh:1#0$$#' tag
        |union all
        |select 'tianjin_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '236992ca26ce4f8dbc1d60e4a7fbd320' id, 'hg0:1' tag
        |union all
        |select 'zhejiang_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '2ae200f4aee699e9b1b34b0b23e8acc8' id, 'k99:12' tag
        |union all
        |select 'anhui_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, 'D916E7DE944E7408D0189C0BB139EDE7' id, 'AH11000901:20' tag
        |""".stripMargin)


    spark.sql(
      s"""
        |create table ${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT} as
        |select 'shandong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, 'bTiZlRzW4GwDa2pnfBe8Cg==' id, '031' tag
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '' tag_limit_version, '100567827' id, 'k9a' tag
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '' tag_limit_version, '100567828' id, 'k9b' tag
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
      s"""
        |create table ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_GD} as
        |select 'guangdong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, 'f1ce75974bf78799cd70e5ae47529910|go4#1177' data
        |""".stripMargin)


    val hb_data = """{"data":"FFFE95584CF8055F4A36AA6315C1DFF4|g2h:1#0$#","day":"20210620","file_name":"20210621_hebei_generic_20210620.txt","flag":"txt","model_type":"generic","source":"hebei_mobile"}"""
    val sichuan_data = """{"day":"20210618","source":"sichuan_mobile","data":"01|01|00001|000370b22bf4cd69cdece324f942b8e1|20210617","flag":"txt","file_name":"hlwg_advertput_20210618_001.txt","model_type":"common"}"""
    spark.sql(
      s"""
         |create table ${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON} as
         |select 'hebei_mobile' source, '20210623' load_day, '20210622' day, 'generic' model_type, '${hb_data}' data
         |union all
         |select 'sichuan_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '${sichuan_data}' data
         |""".stripMargin)


  }

  override def afterAll(): Unit = {
    spark.sql("drop database if exists dm_dpi_master CASCADE")
    stop()
  }


  def prepare(): Unit = {


  }

  test("cost cal") {

    Shandong(ComParam("20210623", "shandong_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_SD}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Unicom(ComParam("20210623", "unicom", "generic", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Jiangsu(ComParam("20210623", "jiangsu_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Henan(ComParam("20210623", "henan_mobile", "generic", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Tianjin(ComParam("20210623", "tianjin_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Zhejiang(ComParam("20210623", "zhejiang_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Anhui(ComParam("20210623", "anhui_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Telecom(ComParam("20210623", "telecom", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_TELECOM}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Guangdong(ComParam("20210623", "guangdong_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_GD}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Hebei(ComParam("20210623", "hebei_mobile", "generic", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    Sichuan(ComParam("20210623", "sichuan_mobile", "common", "20210622", argsGen(s"${PropUtils.HIVE_TABLE_ODS_DPI_MKT_FEEDBACK_INCR_JSON}")), Some(spark)).process().insertIntoHive().upsert2Mysql()

    show()
  }

  def argsGen(incr: String) = {
    Map("local" -> "true", "incrTab" -> s"${incr}", "tagTab" -> s"${PropUtils.HIVE_TABLE_RP_DPI_MKT_DEVICE_TAG_RESULT}",
      "outOfModels" -> "timewindow") ++ Map("startDay" -> "20210601", "mapTabPre" -> "dm_dpi_master.mappingTab_temp", "monthType" -> "true", "toMysql" -> "true")
  }

  def show(): Unit = {
    println("res =>")
    spark.sql(s"select * from ${PropUtils.HIVE_TABLE_CARRIERSIDE_COST}").show(false)
    spark.sql(s"select * from ${PropUtils.HIVE_TABLE_PLATSIDE_COST}").show(false)
    spark.sql(s"select * from ${PropUtils.HIVE_TABLE_CATESIDE_COST}").show(false)
  }
}
