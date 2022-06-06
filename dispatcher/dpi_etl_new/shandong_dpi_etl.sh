#!/bin/bash
set -x -e

cd `dirname $0`
home_dir=`pwd`
model_type=$2
base_dir=/data/dpi/shandong_mobile/download/data/$model_type
dispatcher_check_files=/home/dpi/dpi_feedback/dispatcher/check_files
cd $base_dir
hive_db=dw_dpi_feedback
#hive_table=ods_dpi_mkt_feedback_incr_sd_sb
hive_table=ods_dpi_mkt_feedback_incr_sd

#hive_mapping_db=dw_dpi_feedback
hive_mapping_db=dm_dpi_mapping_test
#hive_mapping_table=dpi_mkt_tag_mapping_shandong_sb
hive_mapping_table=dpi_mkt_tag_mapping_shandong

hive_path=/user/hive/warehouse/${hive_db}.db/${hive_table}
hive_mapping_path=/user/hive/warehouse/${hive_mapping_db}.db/${hive_mapping_table}

data_source=shandong_mobile

load_day=$1

file_list=$3

force="true"

function deal_file(){
  cd $base_dir
  file_path=$1
  file_name=${file_path##*/}
  day=$(echo $file_path|awk -F '_' '{print $(NF-1)}')
  hdfs_path=$hive_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$day
  echo "$file_path put into $hdfs_path"  
  hdfs dfs -mkdir -p $hdfs_path
  hdfs dfs -put -f $file_path $hdfs_path
}

function deal_mapping_file(){
  cd $base_dir
  mapping_file_path=$1
  mapping_file_name=${mapping_file_path##*/}
  mapping_day=$(echo $mapping_file_name|awk -F '/' '{print $NF}'|awk -F '_' '{print $1}')
  mapping_hdfs_path=$hive_mapping_path/load_day=$load_day/source=$data_source/model_type=$model_type/day=$mapping_day
  echo "$mapping_file_path put into $mapping_hdfs_path"  
  hdfs dfs -mkdir -p $mapping_hdfs_path
  hdfs dfs -put -f $mapping_file_path $mapping_hdfs_path
}

if [[ -z $file_list ]];then
 echo "没有新文件"
 exit 0
fi

for file in ${file_list[@]}
do
 resfile=${file%%,*}
 mappingfile=${file##*,}
 if [ "$force" = "false" ];then
 actual_resfile_size=`du -b ${resfile} |awk '{print $1}'`
 verf_resfile_size=`cat ${dispatcher_check_files}${resfile}".dispatcher_verf"`
 if [[ actual_resfile_size -eq 0 || actual_resfile_size -gt verf_resfile_size ]]; then
   echo ${actual_resfile_size} > ${dispatcher_check_files}${resfile}".dispatcher_verf"
   exit 1
 fi
 #deal_file $resfile
 actual_mappingfile_size=`du -b ${mappingfile} |awk '{print $1}'`
 verf_mappingfile_size=`cat ${dispatcher_check_files}${mappingfile}".dispatcher_verf"`
 if [[ actual_mappingfile_size -eq 0 || actual_mappingfile_size -gt verf_mappingfile_size ]]; then
   echo ${actual_mappingfile_size} > ${dispatcher_check_files}${mappingfile}".dispatcher_verf"
   exit 1
 fi
 fi
 deal_file $resfile
 deal_mapping_file $mappingfile
done

cd $home_dir
hive -e "msck repair table ${hive_db}.${hive_table};msck repair table ${hive_mapping_db}.${hive_mapping_table}"

