#!/usr/bin/env bash
set -x -e

if [ $# -eq 0 ]; then
    echo "version-build.sh {version}"
    exit -1
fi
PROJECT_VERSION=$1
DPIANALYZE_HOME="$(cd "`dirname "$0"`"; pwd)"

cd ${DPIANALYZE_HOME}

#echo "__version__ = \"$PROJECT_VERSION\"" > src/main/module/const/version.py

mvn versions:set -DnewVersion=${PROJECT_VERSION}

mvn versions:commit