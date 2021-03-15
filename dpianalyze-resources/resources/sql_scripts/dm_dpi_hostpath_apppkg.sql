
CREATE TABLE IF NOT EXISTS `dm_dpi_mapping_test.dm_dpi_hostpath_apppkg`(
  `apppkg` string COMMENT '清洗后的包',
  `host` string COMMENT 'host',
  `path` string COMMENT 'host'
  )
COMMENT ''
PARTITIONED BY (
  `day` string COMMENT '版本')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';