#!/bin/bash
set -x -e

cd `dirname $0`
hive_db=dw_dpi_feedback
#hive_table=ods_dpi_mkt_feedback_incr_sb
hive_table=ods_dpi_mkt_feedback_incr_json
home_dir=`pwd`
#base_dir=/data/dpi/henan_mobile/download/data
#dispatcher_check_files=/home/dpi/dpi_feedback/dispatcher/check_files
#cd $base_dir
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=sichuan_mobile


load_day=$1
model_type=$2
hdfs_file_list=$3

function deal_hdfs_file(){
#  cd $base_dir
  file_path=$1
#  file_name=${file_path##*/}
  day=$(echo ${file_path}|awk -F '/' '{print $(NF-1)}'|awk -F '=' '{print $(NF)}')
#  tag_limit_version=$(echo ${file_name}|awk -F '_' '{print $(NF-1)}')
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  cd $home_dir
#  tmp_file=./${file_name}
#  cat $file_path > $tmp_file
#  sed -i '1d' $tmp_file
#  sed -i "s/$/|$tag_limit_version/" $tmp_file
  echo "$file_path copy into $hdfs_path"  
  hdfs dfs -mkdir -p $hdfs_path
  hdfs dfs -cp $file_path $hdfs_path
#  rm $tmp_file
}

if [[ -z $hdfs_file_list ]];then
 echo "没有新文件"
 exit 0
fi

for file in ${hdfs_file_list[@]}
do
# 河北hdfs无需校验
# actual_file_size=`du -b ${file} |awk '{print $1}'`
# verf_file_size=`cat ${dispatcher_check_files}${file}".dispatcher_verf"`
# if [[ actual_file_size -eq 0 || actual_file_size -gt verf_file_size ]]; then
#   echo ${actual_file_size} > ${dispatcher_check_files}${file}".dispatcher_verf"
#   exit 1
# fi
 deal_hdfs_file $file
done

cd $home_dir
hive -e "msck repair table ${hive_db}.${hive_table}"

