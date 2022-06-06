#!/bin/bash
set -x -e

cd `dirname $0`
hive_db=dw_dpi_feedback
#hive_table=ods_dpi_mkt_feedback_incr_sb
hive_table=ods_dpi_mkt_feedback_incr
home_dir=`pwd`
base_dir=/data/dpi/henan_mobile/download/data
dispatcher_check_files=/home/dpi/dpi_feedback/dispatcher/check_files
cd $base_dir
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=henan_mobile


load_day=$1
model_type=$2
file_list=$3

function deal_file(){
  cd $base_dir
  file_path=$1
  file_name=${file_path##*/}
  day=$(echo ${file_path}|awk -F '/' '{print $(NF-1)}'|awk -F '=' '{print $(NF)}')
#  tag_limit_version=$(echo ${file_name}|awk -F '_' '{print $(NF-1)}')
  tag_limit_version=""
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$(hdfs dfs -cat ${file_path} | jq '.day'|uniq |tr -d "\""  )
  cd $home_dir
  tmp_file=./${file_name}
  hdfs dfs -cat ${file_path} | jq '.data'|tr -d "\"" > $tmp_file
  #sed -i '1d' $tmp_file
  #sed -i "s/$/|$tag_limit_version/" $tmp_file
  echo "$file_path put into $hdfs_path"  
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
 deal_file $file
done

cd $home_dir
hive -e "msck repair table ${hive_db}.${hive_table}"


