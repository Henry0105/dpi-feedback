#!/usr/bin/env bash
set -x -e
pwd

# mac os readlink -f not work
if [ -z "${DPIANALYZE_HOME}" ]; then
    export DPIANALYZE_HOME="$(readlink -f $(cd "`dirname "$0"`"/..; pwd))"
fi

DPIANALYZE_BIN_HOME="$DPIANALYZE_HOME/sbin"
DPIANALYZE_TMP="$DPIANALYZE_HOME/tmp"
DPIANALYZE_LOG_DIR="$DPIANALYZE_HOME/logs"
DPIANALYZE_CONF_DIR="$DPIANALYZE_HOME/conf"
DPIANALYZE_LIB_DIR="$DPIANALYZE_HOME/lib"

source ${DPIANALYZE_CONF_DIR}/dpianalyze-env.sh

if [ ! -d "$DPIANALYZE_LOG_DIR" ]; then
    mkdir -p "$DPIANALYZE_LOG_DIR"
fi

if [ ! -d "$DPIANALYZE_TMP" ]; then
    mkdir -p "$DPIANALYZE_TMP"
fi

echo ${DPIANALYZE_HOME}
model=$1
source=$2
province=$3
day=${4:-$(date -d '2 days ago' +%Y%m%d)}
mapping=${5:-"false"}
force=${6:-"false"}
imDay=${7:-""}
/opt/mobdata/sbin/spark-submit \
    --verbose \
    --executor-memory 6G \
    --master yarn \
    --deploy-mode cluster \
    --executor-cores 2 \
    --driver-memory 4G \
    --queue root.yarn_dpi.dpi_test \
    --conf spark.kryoserializer.buffer.max=1024m \
    --conf "spark.yarn.executor.memoryOverhead=2048" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Dcom.sun.management.jmxremote.port=27015 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" \
    --conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
    --conf "spark.speculation.quantile=0.9" \
    --conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12" \
    --conf "spark.speculation=true" \
    --conf "spark.shuffle.service.enabled=true"  \
    --files ${DPIANALYZE_CONF_DIR}/log4j.properties,${DPIANALYZE_CONF_DIR}/hive_database_table.properties,${DPIANALYZE_CONF_DIR}/application.properties \
    --jars ${DPIANALYZE_LIB_DIR}/dpianalyze-commons-v1.0.0.jar \
    --class com.mob.dpi.Bootstrap \
    ${DPIANALYZE_LIB_DIR}/dpianalyze-core-v1.0.0.jar \
    --day $day \
    --model ${model} \
    --source ${source} \
    --province ${province} \
    --jobs 1 \
    --mapping ${mapping} \
    --force ${force} \
    --imDay ${imDay}
#######################################################################
# DPIAnalyze
# Usage: com.mob.dpi.Bootstrap$ [options]
#
#   -j, --jobs <value>      任务执行列表, [1-MappingValue], 默认: 1
#   -l, --local             本地模式, 调试用, 默认: false
#   -d, --day <value>       计算日期, yyyyMMdd格式, 默认T-2
#   -m, --model <value>     id类型, 支持: idfa|game|common, 默认: game
#   -p, --province <value>  省份id, 默认: all
#   -s, --source <value>    运营商, 支持: unicom, 默认: unicom
#   -t, --mapping <value>   是否通过表dm_dpi_mapping_test.dm_dpi_mkt_url_tag_comment_extenal进行tags转换
#######################################################################