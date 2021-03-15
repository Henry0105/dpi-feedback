#!/usr/bin/env bash

set -e -x


SCRIPT_HOME="$(cd "`dirname "$0"`"; pwd)/.."
echo "SCRIPT_HOME: ${SCRIPT_HOME}"
load_date=`date +%Y%m%d`
request_date=${1}
#outpath_path="${SCRIPT_HOME}/output/common"


#T-1数据
#data_date=`date -d "yesterday ${load_date}" +%Y%m%d`
#data_date=${load_date}

remotefilename_default="res_imei_youkun_${request_date}.txt"
remotefilename=${2:-"${remotefilename_default}"}


if echo ${remotefilename} | grep "idfa"
then
  model_type="idfa_common"
else
  model_type="common"
fi
echo "${model_type}"

#T-1数据
data_date=`date -d "yesterday ${remotefilename:16:8}" +%Y%m%d`

outpath_path="${SCRIPT_HOME}/output/${model_type}/${load_date}"
new_outpath_path="/data/dpi/zhejiang_mobile/download/data/${model_type}/${load_date}"
cache_path="${SCRIPT_HOME}/cache/${model_type}"


if [ ! -e ${outpath_path} ]; then
  mkdir -p ${outpath_path}
fi

if [ ! -e ${new_outpath_path} ]; then
  mkdir -p ${new_outpath_path}
fi


if [ ! -e ${cache_path} ]; then
  mkdir -p ${cache_path}
fi


if [ -f "${cache_path}/${data_date}" ]; then
  echo "该日期【${data_date}】数据7天内重复请求!"
  exit 0
fi


req1=`curl -G -k -d "userId=shyk_xx&pwd=iv3Quv4zMVOyGF6enNlTnA==&appKey=4188b48fe612683a8f2661d6b2ccf34e" https://bdcplat.zj.chinamobile.com:8000/dataex/api/auth`
token=`echo ${req1}|jq '.token'|sed 's/\"//g'`

if [ -z "${token}" -o "${token}" = "null" ]; then
  echo "TOKEN IS NULL"
  exit 1
fi
echo ${token}
sleep 10s

req2=`curl -G -k -d "token=${token}&appKey=4188b48fe612683a8f2661d6b2ccf34e&fileName=youkun/${remotefilename}" https://bdcplat.zj.chinamobile.com:8000/dataex/api/data/dmpftp1`

if [ -z "${req2}" -o "${req2}" = "null" ]; then
  echo "REQ2 IS NULL,Bad Request!"
  exit 1
fi

error_code2=`echo ${req2}|jq '.error_code'|sed 's/\"//g'`
request2=`echo ${req2}|jq '.request'|sed 's/\"//g'`


if [ -z "${error_code2}" -o -z "${request2}" -o "${error_code2}" = "null" -o "${request2}" = "null" ]; then
  echo "Bad request!"
  exit 1
fi

#if [ ${error_code2} -eq 10033 ]; then
#  echo "重复请求！"
#  exit 0
#fi

echo ${error_code2}
echo ${request2}

sleep 10s
file_name=`echo "${outpath_path}/${remotefilename}"`
echo "file_name:${file_name}"
curl -G -k --speed-limit 1 -o ${file_name} -d "token=${token}&appKey=4188b48fe612683a8f2661d6b2ccf34e" https://bdcplat.zj.chinamobile.com:8000${request2}

#检测文件首行是否正确
head_line_mt=`head -n 1 ${file_name} | egrep -v "^{.*}$"`
if [ -z "${head_line_mt}" ]; then
  echo "file[${file_name}] down load error!"
  exit 1
fi

# 复制到统一目录
cp ${file_name} ${new_outpath_path}

# 最后这部分入hive库会迁移掉
partition_path="/user/hive/warehouse/dw_dpi_feedback.db/ods_dpi_mkt_feedback_incr/load_day=${load_date}/source=zhejiang_mobile/model_type=${model_type}/day=${data_date}"

! `hdfs dfs -test -d ${partition_path}` && hdfs dfs -mkdir -p ${partition_path} && echo "create ${partition_path} success!"

echo "Putting ${file_name} to ${partition_path}"



hadoop fs -put -f ${file_name} ${partition_path}

`hdfs dfs -test -f "${partition_path}/${remotefilename}"` && echo "put success!"
echo "Start to MSCK REPAIR TABLE dw_dpi_feedback.ods_dpi_mkt_feedback_incr ..."
hive -e "MSCK REPAIR TABLE dw_dpi_feedback.ods_dpi_mkt_feedback_incr"
echo "MSCK REPAIR TABLE dw_dpi_feedback.ods_dpi_mkt_feedback_incr success!"

if [ ! -f "${cache_path}/${data_date}" ]; then
  touch "${cache_path}/${data_date}"
fi

find ${cache_path} -type f  -mtime +30 | xargs rm -rvf
echo "Job finish!"
exit 0
