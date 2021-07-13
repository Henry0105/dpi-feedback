#!/usr/bin/env bash
set -x -e
pwd

# mac os readlink -f not work
if [[ -z "${DPIANALYZE_HOME}" ]]; then
    export DPIANALYZE_HOME="$(readlink -f $(cd "`dirname "$0"`"/..; pwd))"
fi

load_day=${1}

/usr/bin/hive -e "\
SET mapreduce.job.queuename=yarn_dpi.dpi_test; \
SET hive.merge.mapfiles=true; \
SET hive.merge.mapredfiles=true; \
SET hive.merge.size.per.task=256000000; \
SET hive.merge.smallfiles.avgsize=256000000; \
insert overwrite table dpi_analysis_test.mappingTab_temp \
select cate_l1, tag, plat \
from \
( \
  select cate_l1, tag, plat \
  from \
  ( \
      select cate_l1, tag, case when plat rlike '智能' and cate_l1 = '其他' then '智能增长线_智赋' when plat rlike '智能' and cate_l1 = '接码欺诈' then '智能增长线_智弈' else plat end as plat \
      from \
      ( \
          select case when cate_l1 rlike '保险' or cate_l1 in ('新闻资讯','科技服务','婚恋交友','医疗健康','航旅','军事应用','智能设备','运动','天气','证券财经','网赚') then '保险' \
           when cate_l1 rlike '游戏' or cate_l1 in ('传奇','三国','仙侠') then '游戏' \
           when cate_l1 rlike '教育' or cate_l1 in ('K12','k12') then '教育' \
           when cate_l1 in ('支付','测试') then '信用卡' \
           when cate_l1 in ('高企','空调地暖','跨境电商','') then '其他' \
           when cate_l1 in ('家装','装修','家装-家博会','家装-婚博会') then '装修' \
           when cate_l1 rlike '培训' then '培训' \
           else cate_l1 end as cate_l1, tag, \
          case when plat rlike '智赋' then '智能增长线_智赋' \
           when plat rlike '智弈' then '智能增长线_智弈' \
           when plat rlike '智能' then '智能增长线_智汇' \
           when plat rlike '金融' then '金融线' \
           when plat rlike '平台|di|DI' then '平台' \
           else plat end as plat \
          from dm_dpi_mapping.dpi_mkt_url_withtag \
          where version like '${load_day}%' \
      )a \
      union all \
      select cate_l1, tag, case when plat rlike '智能' and cate_l1 = '其他' then '智能增长线_智赋' when plat rlike '智能' and cate_l1 = '接码欺诈' then '智能增长线_智弈' else plat end as plat \
      from \
      ( \
          select case when cate_l1 rlike '保险' or cate_l1 in ('新闻资讯','科技服务','婚恋交友','医疗健康','航旅','军事应用','智能设备','运动','天气','证券财经','网赚') then '保险' \
           when cate_l1 rlike '游戏' or cate_l1 in ('传奇','三国','仙侠') then '游戏' \
           when cate_l1 rlike '教育' or cate_l1 in ('K12','k12') then '教育' \
           when cate_l1 in ('支付','测试') then '信用卡' \
           when cate_l1 in ('高企','空调地暖','跨境电商','') then '其他' \
           when cate_l1 in ('家装','装修','家装-家博会','家装-婚博会') then '装修' \
           when cate_l1 rlike '培训' then '培训' \
           else cate_l1 end as cate_l1, tag, \
          case when plat rlike '智赋' then '智能增长线_智赋' \
           when plat rlike '智弈' then '智能增长线_智弈' \
           when plat rlike '智能' then '智能增长线_智汇' \
           when plat rlike '金融' then '金融线' \
           when plat rlike '平台|di|DI' then '平台' \
           else plat end as plat \
          from dm_dpi_mapping.tmp_url_operatorstag \
          where version like '${load_day}%' \
      )a \
  )a \
  group by cate_l1, tag, plat \
  union all \
  select cate_l1, tag, plat  \
  from dpi_analysis_test.mappingTab_temp \
)t \
group by cate_l1, tag, plat; \
"