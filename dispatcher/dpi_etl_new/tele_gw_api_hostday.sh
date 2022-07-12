#!/bin/bash
set -x -e

base_dir=/data/dpi/telecom_gw

cd `dirname $0`
hive_db=dw_dpi_feedback
hive_table=ods_dpi_mkt_feedback_incr_telecom_gw_hostday
home_dir=`pwd`

cd $base_dir

day=$1
key=$2

/opt/jdk1.8.0_45/bin/java -Djava.ext.dirs=./lib:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/:/opt/cloudera/parcels/CDH-5.7.6-1.cdh5.7.6.p0.6/jars/ -cp dpianalyze-core-v1.0.0.jar com.mob.dpi.APIToHiveOrc_HostDay $day $key ${hive_db}.${hive_table}

cd $home_dir
hive -e "msck repair table ${hive_db}.${hive_table}"

