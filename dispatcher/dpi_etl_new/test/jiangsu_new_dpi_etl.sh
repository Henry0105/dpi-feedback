#!/bin/bash


set -x -e
cd `dirname $0`
home_dir=`pwd`


hive_db=dm_dpi_master_test
hive_table=ods_dpi_mkt_feedback_incr
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=jiangsu_mobile_new



load_day=$1
model_type=$2
file_list=$3

echo ==============1:$1=========2:$2========3:$3


function deal_file(){
  file_path=$1
  file_name=${file_path##*/}
  tag_limit_version=$(echo $file_name|awk -F '_' '{print $2}')
  day=$(date -d "$load_day 1 day ago"  +%Y%m%d)
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  echo "$file_path put into $hdfs_path"
  cd $home_dir
  tmp_file=./${file_name}
  cat $file_path > $tmp_file
  #sed -i '1d' $tmp_file
  #sed -i "s/$/|$tag_limit_version/" $tmp_file
  hdfs dfs -mkdir -p $hdfs_path
  hdfs dfs -put -f $tmp_file $hdfs_path
  rm $tmp_file
}

if [[ -z $file_list ]];then
 echo "没有新文件"
 exit 0
fi

# 循环处理 file_list文件，上传到hdfs 并添加hive 分区
for file in ${file_list[@]}
do
 deal_file $file
done

cd $home_dir
#hive -e "msck repair table ${hive_db}.${hive_table}"
hive -e "alter table ${hive_db}.${hive_table} add  if not exists partition(load_day='$load_day',source='$data_source',model_type='$model_type',day='$day');"

