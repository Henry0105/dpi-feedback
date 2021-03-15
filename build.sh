#!/usr/bin/env bash
set -x -e

if [ $# -eq 0 ]; then
    echo "build.sh 215|26"
    exit -1
fi

DEMO_HOME="$(cd "`dirname "$0"`"; pwd)"
cd ${DEMO_HOME}

export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m"

mvn clean scalastyle:check package -P$1

latest=$(readlink -f ${DEMO_HOME}/)
version=$(cat ${DEMO_HOME}/dist/conf/version.properties)

TAG_HOME=/home/xxx/tags/xxx/

if [ -e ${TAG_HOME} ] ; then
  if [ -L ${TAG_HOME}/${version} ] ; then
    unlink ${TAG_HOME}/${version}
  fi
  ln -s ${latest}/dist/ ${TAG_HOME}/${version}
fi
