#!/bin/bash
set -x -e

cd `dirname $0`
home_dir=`pwd`
source $home_dir/conf/application.properties
hive_db=$incr_hive_db
hive_table=ods_dpi_mkt_feedback_incr
base_dir="/data/dpiFeedback/gdyd/download"
dispatcher_check_files=$dispatcher_check_files
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=guangdong_unicom_proxy
model_type=$2
deal_file_num=0
cd $base_dir

load_day=$1

file_list=$3

echo ==============1:$1=========2:$2========3:$3


function deal_file(){
  cd $base_dir
  file_path=$1
  file_name=${file_path##*/}
  tag_limit_version=$(echo $file_name|awk -F '_' '{print $2}')
  day=$(date -d "$load_day 2 day ago"  +%Y%m%d)
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  echo "$file_path put into $hdfs_path"
  cd $home_dir
  tmp_file=./${file_name}
  cat $file_path > $tmp_file
  #sed -i '1d' $tmp_file
  #sed -i "s/$/|$tag_limit_version/" $tmp_file
  sed -i "s#,#:#g" $tmp_file
  hdfs dfs -mkdir -p $hdfs_path
  hdfs dfs -put -f $tmp_file $hdfs_path
  rm $tmp_file
}

if [[ -z $file_list ]];then
 echo "没有新文件"
 exit 0
fi

for file in ${file_list[@]}
do
# actual_file_size=`du -b ${file} |awk '{print $1}'`
# verf_file_size=`cat ${dispatcher_check_files}${file}".dispatcher_verf"`
# if [[ actual_file_size -eq 0 || actual_file_size -gt verf_file_size ]]; then
#   echo ${actual_file_size} > ${dispatcher_check_files}${file}".dispatcher_verf"
#   exit 1
# fi
 deal_file $file
done

cd $home_dir
#hive -e "msck repair table ${hive_db}.${hive_table}"
hive -e "alter table ${hive_db}.${hive_table} add  if not exists partition(load_day='$load_day',source='$data_source',model_type='$model_type',day='$day');"
