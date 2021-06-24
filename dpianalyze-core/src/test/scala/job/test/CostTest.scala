package job.test

import com.mob.dpi.beans.{ComParam, Shandong, Unicom}
import org.apache.spark.sql.LocalSparkSession
import org.scalatest.FunSuite

class CostTest extends FunSuite with LocalSparkSession {


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("drop database if exists opdw4_224 CASCADE")
    spark.sql("create database opdw4_224")

    spark.sql(
      """
        |create table opdw4_224.dpi_mkt_url_withtag as
        |select '信用卡' as cate_l1,'k98' as tag,'金融线' as plat
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.tmp_url_operatorstag as
        |select '教育' as cate_l1,'k97' as tag,'智能增长线_智汇' as plat
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.test1 as
        |select 'shandong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '48f986ef050a5934d574b3dcadbc59dc' id
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '48f986ef050a5934d574b3dcadbc59dc' id
        |""".stripMargin)

    spark.sql(
      """
        |create table opdw4_224.test2 as
        |select 'shandong_mobile' source, '20210623' load_day, '20210622' day, 'common' model_type, '' tag_limit_version, 'k98' tag, '48f986ef050a5934d574b3dcadbc59dc' id
        |union all
        |select 'unicom' source, '20210623' load_day, '20210622' day, 'generic' model_type, '' tag_limit_version, 'k98' tag, '48f986ef050a5934d574b3dcadbc59dc' id
        |""".stripMargin)




  }

  override def afterAll(): Unit = {
    stop()
  }


  def prepare(): Unit = {


  }

  test("cost cal") {


    val otherArgs = Map("local" -> "true", "incrTab" -> "opdw4_224.test1", "tagTab" -> "opdw4_224.test2",
      "mappingTab1" -> "opdw4_224.dpi_mkt_url_withtag", "mappingTab2" -> "opdw4_224.tmp_url_operatorstag")

    new Shandong(ComParam("20210623", "shandong_mobile", "common", "20210622", otherArgs), Some(spark)).process()

    println("res =>")

    spark.sql("select * from carrierSide_temp").show(false)
    spark.sql("select * from platSide_temp").show(false)
    spark.sql("select * from cateSide_temp").show(false)



    new Unicom(ComParam("20210623", "unicom", "generic", "20210622", otherArgs), Some(spark)).process()

    println("res =>")

    spark.sql("select * from carrierSide_temp").show(false)
    spark.sql("select * from platSide_temp").show(false)
    spark.sql("select * from cateSide_temp").show(false)



  }
}
