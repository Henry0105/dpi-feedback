#!/bin/bash
set -x -e

sbin_home=$(cd `dirname $0`;pwd)
cd  $sbin_home/../
home_dir=`pwd`
source $home_dir/conf/carrier-shell.properties
hive_db=${dpi_feedback_db}
hive_table=ods_dpi_mkt_feedback_incr

base_dir="/data/dpi/unicom_new/download"
dispatcher_check_files=$dispatcher_check_files
hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
data_source=unicom

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
  day=$(echo $file_name|awk -F '_' '{print $7}'|awk -F '.' '{print $1}')

  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  echo "$file_path put into $hdfs_path"
  cd $home_dir
  tmp_file=./${data_source}_${model_type}_$day.txt
  cat $file_path > $tmp_file
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
hive -e "alter table ${hive_db}.${hive_table} add  if not exists partition(load_day='$load_day',source='$data_source',model_type='$model_type',day='$day');"



# mac os readlink -f not work
if [ -z "${DPIANALYZE_HOME}" ]; then
    export DPIANALYZE_HOME="$(readlink -f $(cd "`dirname "$0"`"/..; pwd))"
fi

DPIANALYZE_BIN_HOME="$DPIANALYZE_HOME/sbin"
DPIANALYZE_TMP="$DPIANALYZE_HOME/tmp"
DPIANALYZE_LOG_DIR="$DPIANALYZE_HOME/logs"
DPIANALYZE_CONF_DIR="$DPIANALYZE_HOME/conf"
DPIANALYZE_LIB_DIR="$DPIANALYZE_HOME/lib"



if [ ! -d "$DPIANALYZE_LOG_DIR" ]; then
    mkdir -p "$DPIANALYZE_LOG_DIR"
fi

if [ ! -d "$DPIANALYZE_TMP" ]; then
    mkdir -p "$DPIANALYZE_TMP"
fi

DPIANALYZE_BIN_HOME1="$DPIANALYZE_BIN_HOME/device_tag_result.sh"
sh $DPIANALYZE_BIN_HOME1  "timewindow"  "unicom"  "all"  "$load_day" false true "20220706"