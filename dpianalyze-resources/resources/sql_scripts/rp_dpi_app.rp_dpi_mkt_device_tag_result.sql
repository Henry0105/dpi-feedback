CREATE TABLE IF NOT EXISTS `test.rp_dpi_mkt_device_tag_result`(
  `id` string COMMENT '回流id',
  `tag` string COMMENT '标签id',
  `times` bigint COMMENT 'url访问去重数量',
  `merge_times` bigint COMMENT '总访问量',
  `imei` string COMMENT 'imei',
  `idfa` string COMMENT 'idfa',
  `phone` string COMMENT 'phone')
COMMENT '回流数据处理'
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