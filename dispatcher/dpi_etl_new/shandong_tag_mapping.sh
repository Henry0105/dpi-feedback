#!/bin/bash
set -x -e

cd `dirname $0`
home_dir=`pwd`
model_type=$2
base_dir=/data/dpi/shandong_mobile/download/data/$model_type
cd $base_dir
hive_path=/user/hive/warehouse/dm_dpi_mapping_test.db/dpi_mkt_tag_mapping_shandong
data_source=shandong_mobile

task_data=$1
begin_date="${task_data:0:8} ${task_data:8:2}"
end_date=$(date -d "$begin_date +1 hours" "+%Y%m%d %H")
load_day=$(date +%Y%m%d)

file_list=($(find ./*.csv -type f -newermt "$begin_date" ! -newermt "$end_date" -size +0))

function deal_file(){
  cd $base_dir
  file_name=$1
  file_path=`pwd`/$file_name
  day=$(echo $file_name|awk -F '/' '{print $NF}'|awk -F '_' '{print $1}')
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  echo "$file_path put into $hdfs_path"  
  hdfs dfs -mkdir -p $hdfs_path
  hdfs dfs -put $file_path $hdfs_path
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
hive -e "msck repair table dm_dpi_mapping_test.dpi_mkt_tag_mapping_shandong"

