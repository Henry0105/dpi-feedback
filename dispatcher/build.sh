#!/bin/bash

set -e

#if [ $# -lt 1 ]; then
#  echo -e "Usage: build.sh <env> <job>\n\nOptions:\n\tenv\tlocal|test|pre|prod\n\tjob\tgeneric|timewindow"
#  exit 1
#fi

#mvn clean package -P$1 -P$2 -Dmaven.test.skip=true
mvn clean package -Pprod -Dmaven.test.skip=true