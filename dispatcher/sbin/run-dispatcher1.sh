#!/usr/bin/env bash
set -x -e

MODEL_HOME="$(cd "`dirname "$0"`"; pwd)/.."
APP_JAVA_HOME=/opt/jdk1.8.0_45/bin
data_time=$1
#注意改动，这里是将2022-07-07转换成20220707，原理是字符串-替换为空
cal_day=${data_time:0:8}

begin_date=`date -d "${cal_day} -7days" +"%Y%m%d"`
end_date=$cal_day


while [ "$begin_date" -le "$end_date" ];
do
 if [ -z "${cal_day}" ]; then
        echo "cal_day is empty"
        exit 1
 fi

 if [ -z "${DPI_DISPATCHER_HOME}" ]; then
     export DPI_DISPATCHER_HOME="$(cd "`dirname "$0"`"/..; pwd)"
 fi

 lib_path=${MODEL_HOME}/lib
 conf_path=${MODEL_HOME}/conf
 ${APP_JAVA_HOME}/java -jar ${lib_path}/dispatcher-1.0-SNAPSHOT.jar -j 0 -d ${begin_date}

 #这里将begin_date+1，用于while循环
 begin_date=$(date -d "${begin_date}+1days" +%Y%m%d)
done
exit 0
