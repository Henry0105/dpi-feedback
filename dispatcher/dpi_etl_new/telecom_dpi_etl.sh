#!/bin/bash
set -x -e

cd `dirname $0`
hive_db=dw_dpi_feedback
hive_table=ods_dpi_mkt_feedback_incr_telecom
home_dir=`pwd`

#/data/dpi/telecom/download/20211027/telecom_mob_20211027.txt
base_dir=/data/dpi/telecom/download
dispatcher_check_files=/home/dpi/dpi_feedback/dispatcher/check_files
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=telecom
cd $base_dir

load_day=$1

file_list=$3

function deal_file(){
  cd $base_dir
  file_path=$1
  file_name=${file_path##*/}
  model_type=common
  #day=$(echo $file_name|awk -F '_' '{print $NF}'|awk -F '.' '{print $1}')
  day=$(date -d "$load_day -1 day" +%Y%m%d)
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  cd $home_dir
  tmp_file=./${data_source}_${model_type}_$day.txt
  cat $file_path |awk -F "," '{print $1"|"$2}' > $tmp_file
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
 #actual_file_size=`du -b ${file} |awk '{print $1}'`
 #verf_file_size=`cat ${dispatcher_check_files}${file}".dispatcher_verf"`
 #if [[ actual_file_size -eq 0 || actual_file_size -gt verf_file_size ]]; then
 #  echo ${acture_file_size} > ${dispatcher_check_files}${file}".dispatcher_verf"
 #  exit 1
 #fi
 deal_file ${file}
done

cd $home_dir
hive -e "msck repair table ${hive_db}.${hive_table}"

# mac os readlink -f not work
DPIANALYZE_HOME=${dpianalyze_home}
sh $DPIANALYZE_BIN_HOME/device_tag_result.sh "common"  "telecom"  "all"  "$load_day" false true "20220510"
