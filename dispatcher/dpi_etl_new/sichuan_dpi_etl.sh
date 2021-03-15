#!/bin/bash
set -x -e

cd `dirname $0`
home_dir=`pwd`
base_dir=/data/dpi/sichuan_mobile/download/data
hive_path=/user/hive/warehouse/dw_dpi_feedback.db/ods_dpi_mkt_feedback_incr
data_source=sichuan_mobile
cd $base_dir

task_data=$1
begin_date="${task_data:0:8} ${task_data:8:2}"
end_date=$(date -d "$begin_date +1 hours" "+%Y%m%d %H")
load_day=$(date +%Y%m%d)

file_list=($(find ./ -type f -newermt "$begin_date" ! -newermt "$end_date" -size +0))

function deal_file(){
  cd $base_dir
  file_name=$1
  file_path=`pwd`/$file_name
  model_type=common
  day=$(echo $file_name|awk -F '_' '{print $NF}'|awk -F '.' '{print $1}')
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  cd $home_dir
  tmp_file=./${data_source}_${model_type}_$day.txt
  cat $file_path |awk -F "|" '{print $2"|"$3}' > $tmp_file
  echo "$file_path put into $hdfs_path"
  hdfs dfs -mkdir -p $hdfs_path
  hdfs dfs -put $tmp_file $hdfs_path
  rm $tmp_file
}

if [[ -z $file_list ]];then
 echo "$begin_date点没有新文件"
 exit 0
fi

for file in ${file_list[@]}
do
 deal_file $file
done

cd $home_dir
hive -e "msck repair table dw_dpi_feedback.ods_dpi_mkt_feedback_incr"

