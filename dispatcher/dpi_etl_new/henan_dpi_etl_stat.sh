#!/bin/bash
set -x -e

cd `dirname $0`
hive_db=dw_dpi_feedback
#hive_table=ods_dpi_mkt_feedback_incr_sb
hive_table=ods_dpi_mkt_feedback_incr_stat
home_dir=`pwd`
dispatcher_check_files=/home/dpi/dpi_feedback/dispatcher/check_files
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=henan_mobile


load_day=$1
model_type=$2
file_list=($3)

function deal_file(){
  file_path=$1
  file_name=${file_path##*/}
  day=$(echo ${file_path}|awk -F '/' '{print $(NF-1)}'|awk -F '=' '{print $(NF)}')
#  tag_limit_version=$(echo ${file_name}|awk -F '_' '{print $(NF-1)}')
  tag_limit_version=""
  hdfs_path=${hive_path}/load_day=${load_day}/source=${data_source}/model_type=${model_type}/day=$(hdfs dfs -cat ${file_path} | jq '.day'|uniq |tr -d "\""  )
  cd ${home_dir}
  tmp_file=./${file_name}
  hdfs dfs -cat ${file_path} | jq '.data'|tr -d "\"" > $tmp_file
  #sed -i '1d' $tmp_file
  sed -i "s/$/|$tag_limit_version/" ${tmp_file}
  echo "$file_path put into $hdfs_path"  
  hdfs dfs -mkdir -p ${hdfs_path}
  hdfs dfs -put -f ${tmp_file} ${hdfs_path}
  rm ${tmp_file}
}

if [[ ${#file_list[@]} -eq 0 ]];then
 echo "没有新文件"
 exit 0
fi

for file in ${file_list[@]}
do
# actual_file_size=`du -b ${file} |awk '{print $1}'`
# verf_file_size=`cat ${dispatcher_check_files}${file}".dispatcher_verf"`
# if [[ actual_file_size -eq 0 || actual_file_size -gt verf_file_size ]]; then
#   echo ${acture_file_size} > ${dispatcher_check_files}${file}".dispatcher_verf"
#   exit 1
# fi
 deal_file ${file}
done

cd ${home_dir}
hive -e "msck repair table ${hive_db}.${hive_table}"


