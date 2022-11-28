#!/bin/bash
set -eu
# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRG_DIR=`dirname "$PRG"`
APP_DIR=`cd "$PRG_DIR/.." >/dev/null; pwd`
CONF_DIR=${APP_DIR}/config
APP_JAR=${APP_DIR}/lib/xuanwu-flink-sysnc.jar
APP_MAIN="com.xuanwu.submit.flink.sql.FlinkSqlStarter"

if [ -f "${CONF_DIR}/xuanwu-env.sh" ]; then
    . "${CONF_DIR}/xuanwu-env.sh"
fi

if [ $# == 0 ]
then
    args="-h"
else
    args=$@
fi


CMD=$(java -cp ${APP_JAR} ${APP_MAIN} ${args}) && EXIT_CODE=$? || EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 234 ]; then
    # print usage
    echo "${CMD}"
    exit 0
elif [ ${EXIT_CODE} -eq 0 ]; then
    echo "Execute xuanwu Flink SQL Job: ${CMD}"
    eval ${CMD}
else
    echo "${CMD}"
    exit ${EXIT_CODE}
fi