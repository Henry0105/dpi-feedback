--  人工维护
--  spark.read.csv("/user/dpi_test/unicom-tags/mob_host.txt").toDF("host").createOrReplaceTempView("t")
--  spark.sql(
--    """
--      |insert overwrite table dm_dpi_mapping_test.dm_dpi_mob_host partition (version='20191105.1000')
--      |select trim(host) as host from t
--    """.stripMargin)
CREATE TABLE IF NOT EXISTS `dm_dpi_mapping_test.dm_dpi_mob_host`(
  `host` string COMMENT 'mob host')
COMMENT 'http://c.mob.com/pages/viewpage.action?pageId=23825272'
PARTITIONED BY (
  `version` string COMMENT '版本')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

create view dm_dpi_mapping_test.dm_dpi_mob_host_view as
select host from dm_dpi_mapping_test.dm_dpi_mob_host where version='20191105.1000'