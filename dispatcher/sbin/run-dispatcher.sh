#!/usr/bin/env bash
set -x -e

MODEL_HOME="$(cd "`dirname "$0"`"; pwd)/.."
APP_JAVA_HOME=/opt/jdk1.8.0_45/bin
data_time=$1
cal_day=${data_time:0:8}

if [ -z "${cal_day}" ]; then
	echo "cal_day is empty"
	exit 1
fi

if [ -z "${DPI_DISPATCHER_HOME}" ]; then
    export DPI_DISPATCHER_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

lib_path=${MODEL_HOME}/lib
conf_path=${MODEL_HOME}/conf
${APP_JAVA_HOME}/java -jar ${lib_path}/dispatcher-1.0-SNAPSHOT.jar -j 0 -d ${cal_day}
exit 0
