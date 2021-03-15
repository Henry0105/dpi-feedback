CREATE TABLE IF NOT EXISTS `rp_dpi_app_test.rp_dpi_imei_duid_mapping`(
  `id` string COMMENT 'imei|idfa',
  `phone` string COMMENT 'phone',
  `id_type` string COMMENT '设备号类型，imei/idfa等',
  `plat` string COMMENT 'plat',
  `duid` string COMMENT 'duid')
COMMENT 'imei-duid映射表'
PARTITIONED BY (
  `day` string COMMENT '数据生成日期',
  `source` string COMMENT '数据来源',
  `model_type` string COMMENT '模型类型')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';