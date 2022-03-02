#!/bin/bash
set -x -e

cd `dirname $0`
hive_db=dw_dpi_feedback
hive_table=?
home_dir=`pwd`

cd $base_dir

load_day=$1


java -cp ${DPIANALYZE_HOME}/lib/dpianalyze-core-v1.0.0.jar com.mob.dpi.APIToHiveOrc \
20220221 ${hive_db}.${hive_table}

cd $home_dir
hive -e "msck repair table ${hive_db}.${hive_table}"

