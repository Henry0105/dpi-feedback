
-- 生成逻辑 dm_dpi_unicom_appkey.sh
CREATE TABLE IF NOT EXISTS `dm_dpi_mapping_test.dm_dpi_mob_appkey`(
  `appkey` string COMMENT 'appkey',
  `apppkg` string COMMENT '清洗后的apppkg',
  `app_name` string COMMENT 'app名称')
COMMENT ''
PARTITIONED BY (
  `version` string COMMENT '数据生成日期做为版本号')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';