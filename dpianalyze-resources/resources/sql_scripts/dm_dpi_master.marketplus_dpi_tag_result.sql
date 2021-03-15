CREATE TABLE IF NOT EXISTS `dm_dpi_master.marketplus_dpi_tag_result`(
  `id` string COMMENT 'mob生成的id,id+tag为唯一键',
  `tag` string COMMENT '标签',
  `times` string COMMENT '标签出现次数',
  `ieid` string COMMENT 'imei, md5(imei_14)',
  `ifid` string COMMENT 'idfa，小写（注意查看是否需要转换）',
  `pid` string COMMENT 'phone，单个手机号')
PARTITIONED BY (
  `load_day` string COMMENT '数据入库日期',
  `source` string COMMENT '数据源，不同运营商',
  `model_type` string COMMENT '模型',
  `day` string COMMENT '数据计算日期')
stored as orc;